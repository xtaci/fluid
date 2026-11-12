// Copyright (c) 2026 xtaci
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package fluid provides an authenticated, acknowledged message transport over a
// transparent serial byte stream.
package fluid

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/awnumar/memguard"
	"github.com/xssnick/raptorq"
	"go.bug.st/serial"
)

const (
	maxSourceSymbols = 56403
	maxSymbolID      = 1<<24 - 1
	maxSymbolIDLimit = maxSymbolID + 1
)

// validSymbolID reports whether id fits the RaptorQ 24-bit encoding-symbol ID.
func validSymbolID(id uint32) bool {
	return id <= maxSymbolID
}

// Config must use the same Key and compatible frame and message sizes at both
// endpoints. MaxWireFrameBytes includes COBS framing and its delimiter; it must
// fit the radio module's maximum payload.
type Config struct {
	// Key is the shared HMAC key; it must contain at least 32 bytes. New copies
	// it into protected memory; the caller remains responsible for its slice.
	Key []byte
	// MaxWireFrameBytes limits one complete encoded frame, including the COBS
	// delimiter. It must be between 76 and 65535; zero defaults to 255.
	MaxWireFrameBytes int
	// MaxAppMessageBytes limits the complete application message size sent or
	// received, excluding per-frame headers and framing. Zero defaults to
	// 1 MiB; empty messages are not supported.
	MaxAppMessageBytes int
	// AckTimeout is the time to wait for a message-completion
	// acknowledgment after sending a source-symbol stream or repair batch.
	// Zero defaults to 3 seconds.
	AckTimeout time.Duration
	// MaxRetries is the maximum number of repair-symbol batches per message after
	// the initial source-symbol stream. Zero defaults to 5.
	MaxRetries int
	// InitialRepairSymbols is the number of repair symbols sent immediately
	// after each source-symbol stream, before waiting for an acknowledgment.
	// Zero disables proactive repairs. The sender stops early if it receives an
	// acknowledgment; radio buffering and half-duplex turnaround may require
	// a small value or zero.
	InitialRepairSymbols int
	// ReceiveTimeout is the maximum idle time before an incomplete incoming
	// message is discarded. Zero defaults to 2 minutes.
	ReceiveTimeout time.Duration
	// ReplayWindow is the retention period for completed message IDs used for
	// deduplication. Zero defaults to 10 minutes.
	ReplayWindow time.Duration
}

// DefaultConfig returns the baseline settings for a transparent serial radio
// transport. It assumes the radio accepts wire frames up to 255 bytes; callers must
// adjust this and the timeouts for their module and air settings. Set Key before
// passing the config to Open or New.
func DefaultConfig() Config {
	return Config{
		MaxWireFrameBytes:  255,
		MaxAppMessageBytes: 1 << 20,
		AckTimeout:         3 * time.Second,
		MaxRetries:         5,
		ReceiveTimeout:     2 * time.Minute,
		ReplayWindow:       10 * time.Minute,
	}
}

// normalized fills zero-valued options from DefaultConfig and validates limits.
// InitialRepairSymbols remains zero when proactive repair is disabled.
func (c Config) normalized() (Config, error) {
	if len(c.Key) < 32 {
		return c, errors.New("key must contain at least 32 bytes")
	}
	defaults := DefaultConfig()
	if c.MaxWireFrameBytes == 0 {
		c.MaxWireFrameBytes = defaults.MaxWireFrameBytes
	}
	if c.MaxAppMessageBytes == 0 {
		c.MaxAppMessageBytes = defaults.MaxAppMessageBytes
	}
	if c.AckTimeout == 0 {
		c.AckTimeout = defaults.AckTimeout
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = defaults.MaxRetries
	}
	if c.ReceiveTimeout == 0 {
		c.ReceiveTimeout = defaults.ReceiveTimeout
	}
	if c.ReplayWindow == 0 {
		c.ReplayWindow = defaults.ReplayWindow
	}
	if c.MaxWireFrameBytes < 76 || c.MaxWireFrameBytes > 65535 ||
		c.MaxAppMessageBytes < 1 || uint64(c.MaxAppMessageBytes) > uint64(^uint32(0))-65535 ||
		c.AckTimeout < 0 || c.MaxRetries < 0 || c.InitialRepairSymbols < 0 ||
		uint64(c.InitialRepairSymbols) > maxSymbolIDLimit ||
		c.ReceiveTimeout < 0 || c.ReplayWindow < 0 {
		return c, errors.New("invalid message configuration")
	}
	return c, nil
}

// Message owns its stream. Close must unblock concurrent Read and Write calls.
// One outgoing and one incoming message may be in progress at a time.
type Message struct {
	stream   io.ReadWriteCloser
	cfg      Config
	key      *memguard.LockedBuffer
	origin   [8]byte
	sendMu   chan struct{}
	writeMu  sync.Mutex
	keyMu    sync.RWMutex
	mu       sync.Mutex
	err      error
	acks     chan frame
	messages chan []byte
	done     chan struct{}
	once     sync.Once
}

// Open opens a transparent serial port using 8N1 and the supplied baud rate.
// The returned Message owns the port and closes it when Message.Close is called.
func Open(port string, baudRate int, cfg Config) (*Message, error) {
	if port == "" || baudRate <= 0 {
		return nil, errors.New("invalid serial port or baud rate")
	}
	if _, err := cfg.normalized(); err != nil {
		return nil, err
	}
	p, err := serial.Open(port, &serial.Mode{BaudRate: baudRate, DataBits: 8, Parity: serial.NoParity, StopBits: serial.OneStopBit})
	if err != nil {
		return nil, fmt.Errorf("open serial port: %w", err)
	}
	l, err := New(p, cfg)
	if err != nil {
		if closeErr := p.Close(); closeErr != nil {
			return nil, errors.Join(err, fmt.Errorf("close serial port: %w", closeErr))
		}
		return nil, err
	}
	return l, nil
}

// New starts a message transport over a transparent byte stream and takes
// ownership of it on success. The stream's Close method must unblock pending
// reads and writes. The key is copied into protected memory; the caller retains
// ownership of the original key slice.
func New(stream io.ReadWriteCloser, cfg Config) (*Message, error) {
	if stream == nil {
		return nil, errors.New("nil stream")
	}
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	// Retain the key only in locked memory, not in the stored configuration.
	key := memguard.NewBuffer(len(cfg.Key))
	if key.Size() != len(cfg.Key) {
		return nil, errors.New("allocate protected key memory")
	}
	copy(key.Bytes(), cfg.Key)
	key.Freeze()
	cfg.Key = nil
	l := &Message{stream: stream, cfg: cfg, sendMu: make(chan struct{}, 1),
		key: key, acks: make(chan frame, 1), messages: make(chan []byte, 1), done: make(chan struct{})}
	if _, err := rand.Read(l.origin[:]); err != nil {
		key.Destroy()
		return nil, fmt.Errorf("generate message origin: %w", err)
	}
	l.sendMu <- struct{}{}
	go l.readLoop()
	return l, nil
}

// Close closes the underlying stream and unblocks pending operations. It
// returns any previously recorded terminal error or stream-close error.
func (l *Message) Close() error {
	l.fail(io.ErrClosedPipe)
	if err := l.failure(); err != io.ErrClosedPipe {
		return err
	}
	return nil
}

// fail records the first terminal error, closes the stream exactly once, and
// destroys the protected key after in-flight cryptographic operations finish.
func (l *Message) fail(err error) {
	l.once.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		// Wake waiters and unblock stream I/O before waiting for key users.
		close(l.done)
		if closeErr := l.stream.Close(); closeErr != nil {
			l.mu.Lock()
			l.err = errors.Join(l.err, fmt.Errorf("close stream: %w", closeErr))
			l.mu.Unlock()
		}
		l.keyMu.Lock()
		if l.key != nil && l.key.IsAlive() {
			l.key.Destroy()
		}
		l.keyMu.Unlock()
	})
}

// failure returns the recorded terminal error, or nil if none has been recorded.
func (l *Message) failure() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// write authenticates and writes a frame without interleaving other frames.
// A stream write failure is terminal for the Message.
func (l *Message) write(f frame) error {
	select {
	case <-l.done:
		return l.failure()
	default:
	}
	// Hold the key lock while marshaling so Close cannot destroy it.
	l.keyMu.RLock()
	select {
	case <-l.done:
		l.keyMu.RUnlock()
		return l.failure()
	default:
	}
	if l.key == nil || !l.key.IsAlive() {
		l.keyMu.RUnlock()
		return errors.New("protected key is unavailable")
	}
	packet := marshalFrame(f, l.key.Bytes())
	l.keyMu.RUnlock()
	if len(packet) > l.cfg.MaxWireFrameBytes {
		return errors.New("encoded frame exceeds maximum frame size")
	}
	// Keep the lock across partial writes to preserve frame boundaries.
	l.writeMu.Lock()
	defer l.writeMu.Unlock()
	for len(packet) > 0 {
		n, err := l.stream.Write(packet)
		if err != nil {
			l.fail(fmt.Errorf("write stream: %w", err))
			return l.failure()
		}
		if n <= 0 || n > len(packet) {
			l.fail(io.ErrShortWrite)
			return l.failure()
		}
		packet = packet[n:]
	}
	return nil
}

// parse decodes and authenticates a wire frame with the message's protected key.
func (l *Message) parse(encoded []byte) (frame, error) {
	l.keyMu.RLock()
	defer l.keyMu.RUnlock()
	if l.key == nil || !l.key.IsAlive() {
		return frame{}, errors.New("protected key is unavailable")
	}
	return parseFrame(encoded, l.key.Bytes())
}

// symbolSize returns the largest symbol payload that fits in one configured
// wire frame after headers, authentication, COBS encoding, and delimiter.
func (l *Message) symbolSize() int {
	// Reserve the worst-case COBS overhead, including the zero delimiter.
	return l.cfg.MaxWireFrameBytes - headerSize - macSize - l.cfg.MaxWireFrameBytes/254 - 2
}

// Send encodes a nonempty message in one RaptorQ generation. The message must
// fit both MaxAppMessageBytes and the generation's source-symbol limit. It
// returns nil only after the receiver has reassembled and queued the message
// and the sender has received its completion acknowledgment. Cancellation
// does not guarantee that the peer has not received the message.
func (l *Message) Send(ctx context.Context, data []byte) error {
	if len(data) == 0 || len(data) > l.cfg.MaxAppMessageBytes ||
		uint64(len(data)) > uint64(maxSourceSymbols)*uint64(l.symbolSize()) {
		return errors.New("message length is outside configured limits")
	}
	size := l.symbolSize()
	// Serialize messages while allowing cancellation before the lock is acquired.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
		return l.failure()
	case <-l.sendMu:
	}
	defer func() { l.sendMu <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Use a random message suffix and a per-link origin to distinguish messages
	// and identify data frames echoed by the local serial module.
	f := frame{kind: frameData, messageLen: uint32(len(data)), symbolSize: uint16(size)}
	if _, err := rand.Read(f.id[:]); err != nil {
		return fmt.Errorf("generate message ID: %w", err)
	}
	copy(f.id[:len(l.origin)], l.origin[:])
	enc, err := raptorq.NewRaptorQ(uint32(size)).CreateEncoder(data)
	if err != nil {
		return fmt.Errorf("create RaptorQ encoder: %w", err)
	}
	_, err = l.sendGeneration(ctx, f, enc)
	return err
}

// sendGeneration streams source and repair symbols and waits for a matching
// completion ACK. It reports whether the message was acknowledged; an
// exhausted retry budget or symbol-ID space returns an error.
func (l *Message) sendGeneration(ctx context.Context, f frame, enc *raptorq.Encoder) (bool, error) {
	sourceSymbols := enc.BaseSymbolsNum()
	// Stream all source symbols without waiting for per-symbol acknowledgments.
	for f.index = 0; f.index < sourceSymbols; f.index++ {
		if acked, err := l.checkMessageAck(ctx, f); err != nil {
			return false, err
		} else if acked {
			return true, nil
		}
		f.payload = enc.GenSymbol(f.index)
		if err := l.write(f); err != nil {
			return false, fmt.Errorf("send source symbol %d: %w", f.index, err)
		}
		if acked, err := l.checkMessageAck(ctx, f); err != nil {
			return false, err
		} else if acked {
			return true, nil
		}
	}

	// Repair IDs follow source IDs and must remain within RaptorQ's 24-bit range.
	// Proactive repairs can recover loss without waiting for the first timeout.
	nextRepair := sourceSymbols
	initialEnd := uint64(nextRepair) + uint64(l.cfg.InitialRepairSymbols)
	if initialEnd > maxSymbolIDLimit {
		initialEnd = maxSymbolIDLimit
	}
	for uint64(nextRepair) < initialEnd {
		if acked, err := l.checkMessageAck(ctx, f); err != nil {
			return false, err
		} else if acked {
			return true, nil
		}
		f.index = nextRepair
		f.payload = enc.GenSymbol(f.index)
		if err := l.write(f); err != nil {
			return false, fmt.Errorf("send initial repair symbol %d: %w", f.index, err)
		}
		nextRepair++
	}
	if acked, err := l.waitMessageAck(ctx, f); err != nil {
		return false, err
	} else if acked {
		return true, nil
	}

	// Start at one quarter of the source symbols (rounded up), then double
	// each batch until decoding is acknowledged or the retry budget is exhausted.
	batchSize := (sourceSymbols + 3) / 4
	if batchSize == 0 {
		batchSize = 1
	}
	for attempt := 0; attempt < l.cfg.MaxRetries && nextRepair < maxSymbolIDLimit; attempt++ {
		batchEnd := nextRepair + batchSize
		if batchEnd < nextRepair || batchEnd > maxSymbolIDLimit {
			batchEnd = maxSymbolIDLimit
		}
		for f.index = nextRepair; f.index < batchEnd; f.index++ {
			if acked, err := l.checkMessageAck(ctx, f); err != nil {
				return false, err
			} else if acked {
				return true, nil
			}
			f.payload = enc.GenSymbol(f.index)
			if err := l.write(f); err != nil {
				return false, fmt.Errorf("send repair symbol %d: %w", f.index, err)
			}
			if acked, err := l.checkMessageAck(ctx, f); err != nil {
				return false, err
			} else if acked {
				return true, nil
			}
		}
		nextRepair = batchEnd
		if acked, err := l.waitMessageAck(ctx, f); err != nil {
			return false, err
		} else if acked {
			return true, nil
		}
		if batchSize <= maxSymbolIDLimit/2 {
			batchSize *= 2
		} else {
			batchSize = maxSymbolIDLimit
		}
	}
	return false, errors.New("message acknowledgment timeout")
}

// checkMessageAck consumes queued ACKs without blocking, ignoring those that
// do not match the expected message. Cancellation and terminal errors are returned.
func (l *Message) checkMessageAck(ctx context.Context, expected frame) (bool, error) {
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-l.done:
			return false, l.failure()
		case ack := <-l.acks:
			if isMessageAck(ack, expected) {
				return true, nil
			}
		default:
			return false, nil
		}
	}
}

// waitMessageAck waits up to AckTimeout for an ACK matching the expected
// message. A timeout returns false without an error so the caller can retry.
func (l *Message) waitMessageAck(ctx context.Context, expected frame) (bool, error) {
	timer := time.NewTimer(l.cfg.AckTimeout)
	defer timer.Stop()
	for {
		if acked, err := l.checkMessageAck(ctx, expected); acked || err != nil {
			return acked, err
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-l.done:
			return false, l.failure()
		case ack := <-l.acks:
			if isMessageAck(ack, expected) {
				return true, nil
			}
		case <-timer.C:
			return false, nil
		}
	}
}

// isMessageAck reports whether ack matches the message ID and generation
// metadata. The symbol index does not affect a message-completion ACK.
func isMessageAck(ack, expected frame) bool {
	return ack.id == expected.id && ack.messageLen == expected.messageLen &&
		ack.symbolSize == expected.symbolSize && ack.kind == frameAck
}

// Receive waits for the next completely reassembled message, cancellation, or
// a terminal transport error. A queued message takes precedence over closure.
func (l *Message) Receive(ctx context.Context) ([]byte, error) {
	// Prefer a queued message even when the message has already closed.
	select {
	case msg := <-l.messages:
		return msg, nil
	default:
	}
	select {
	case msg := <-l.messages:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.done:
		select {
		case msg := <-l.messages:
			return msg, nil
		default:
			return nil, l.failure()
		}
	}
}

// incoming holds the state for the message currently being reassembled.
type incoming struct {
	// id identifies the message.
	id [16]byte
	// messageLen describes the complete application message.
	messageLen uint32
	// assembled contains the decoded message.
	assembled []byte
	decoder   *raptorq.Decoder
	// deliveryReady means the message is decoded but the application queue
	// was full when delivery was attempted.
	deliveryReady bool
	// symbolSize is the symbol size of this message.
	symbolSize uint16
	// updated is the time the most recent symbol or retry was received.
	updated time.Time
}

// completed stores metadata for delivered messages so late symbols can trigger
// another completion acknowledgment without duplicate delivery.
type completed struct {
	// messageLen and symbolSize identify the completed message's shape.
	messageLen uint32
	symbolSize uint16
	// expires is when this deduplication entry can be discarded.
	expires time.Time
}

// readLoop extracts delimited frames, authenticates them, and dispatches them.
// Invalid frames are discarded; oversized frames are discarded through the next
// delimiter for resynchronization. Stream and decoder errors terminate the Message.
func (l *Message) readLoop() {
	var active *incoming
	completedIDs := make(map[[16]byte]completed)
	buf := make([]byte, 1024)
	packet := make([]byte, 0, l.cfg.MaxWireFrameBytes)
	overflow := false
	for {
		n, err := l.stream.Read(buf)
		// Discard an oversized packet until the next delimiter to resynchronize.
		for _, b := range buf[:n] {
			if b != 0 {
				if len(packet) >= l.cfg.MaxWireFrameBytes-1 {
					overflow = true
				} else if !overflow {
					packet = append(packet, b)
				}
				continue
			}
			// Only authenticated frames can affect acknowledgment or receive state.
			if !overflow && len(packet) > 0 {
				f, parseErr := l.parse(packet)
				if parseErr == nil {
					if f.kind == frameAck && len(f.payload) == 0 {
						// Never block the reader if an ACK is already queued.
						select {
						case l.acks <- f:
						default:
						}
					} else if f.kind == frameData {
						if handleErr := l.handleData(f, &active, completedIDs); handleErr != nil {
							l.fail(handleErr)
							return
						}
					}
				}
			}
			packet = packet[:0]
			overflow = false
		}
		// Process bytes returned with an error before terminating the transport.
		if err != nil {
			l.fail(fmt.Errorf("read stream: %w", err))
			return
		}
		if n == 0 {
			l.fail(io.ErrNoProgress)
			return
		}
	}
}

// handleData validates an authenticated symbol and advances the one active
// receive generation. It ignores invalid or competing messages and acknowledges
// a completed message only after it has been queued.
func (l *Message) handleData(f frame, current **incoming, completedIDs map[[16]byte]completed) error {
	// Ignore local echoes and expire old deduplication entries.
	if bytes.Equal(f.id[:len(l.origin)], l.origin[:]) {
		return nil
	}
	now := time.Now()
	for id, entry := range completedIDs {
		if !now.Before(entry.expires) {
			delete(completedIDs, id)
		}
	}
	// Validate all size and index fields before allocating a decoder.
	if f.messageLen == 0 || uint64(f.messageLen) > uint64(l.cfg.MaxAppMessageBytes) ||
		f.symbolSize == 0 || int(f.symbolSize) > l.symbolSize() || len(f.payload) != int(f.symbolSize) {
		return nil
	}
	sourceSymbols := (uint64(f.messageLen) + uint64(f.symbolSize) - 1) / uint64(f.symbolSize)
	if sourceSymbols > maxSourceSymbols || !validSymbolID(f.index) {
		return nil
	}
	// Acknowledge retries for completed messages without delivering them again.
	if entry, ok := completedIDs[f.id]; ok {
		if entry.messageLen == f.messageLen && entry.symbolSize == f.symbolSize {
			return l.ack(f, frameAck)
		}
		return nil
	}
	// Accept a new message only after the active receive state expires.
	if *current != nil && now.Sub((*current).updated) >= l.cfg.ReceiveTimeout {
		*current = nil
	}
	if *current == nil {
		// Do not evict unexpired IDs: doing so could deliver a retry twice.
		if len(completedIDs) >= 128 {
			return nil
		}
		*current = &incoming{id: f.id, messageLen: f.messageLen,
			symbolSize: f.symbolSize, updated: now}
	}
	s := *current
	if s.id != f.id || s.messageLen != f.messageLen ||
		s.symbolSize != f.symbolSize {
		return nil
	}
	s.updated = now
	if s.deliveryReady {
		// A retry provides another opportunity to deliver after queue backpressure.
		return l.deliverIncoming(s, current, completedIDs, f, now)
	}
	if s.decoder == nil {
		dec, err := raptorq.NewRaptorQ(uint32(f.symbolSize)).CreateDecoder(f.messageLen)
		if err != nil {
			return fmt.Errorf("create RaptorQ decoder: %w", err)
		}
		s.decoder = dec
	}
	// A ready decoder may still need more independent repair symbols to decode.
	ready, err := s.decoder.AddSymbol(f.index, f.payload)
	if err != nil {
		return fmt.Errorf("add RaptorQ symbol: %w", err)
	}
	if !ready {
		return nil
	}
	ok, msg, err := s.decoder.Decode()
	if err != nil {
		return fmt.Errorf("decode RaptorQ message: %w", err)
	}
	if !ok {
		return nil
	}
	s.assembled = msg
	s.decoder = nil
	if uint64(len(s.assembled)) != uint64(s.messageLen) {
		return errors.New("reassembled message length mismatch")
	}
	s.deliveryReady = true
	return l.deliverIncoming(s, current, completedIDs, f, now)
}

// deliverIncoming queues a reassembled message, records its replay metadata,
// and sends its final ACK. If the queue is full, it retains the active message
// without acknowledging it so a subsequent retry can attempt delivery again.
func (l *Message) deliverIncoming(s *incoming, current **incoming, completedIDs map[[16]byte]completed, f frame, now time.Time) error {
	select {
	case l.messages <- s.assembled:
		completedIDs[s.id] = completed{s.messageLen, s.symbolSize, now.Add(l.cfg.ReplayWindow)}
		*current = nil
		return l.ack(f, frameAck)
	default:
		return nil
	}
}

// ack sends an authenticated acknowledgment with no symbol payload.
func (l *Message) ack(f frame, kind byte) error {
	f.kind = kind
	f.payload = nil
	return l.write(f)
}
