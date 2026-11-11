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

// Package fluid provides an authenticated, acknowledged link over a
// transparent serial byte stream.
package fluid

import (
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

// Config must use the same Key and compatible frame and chunk sizes at both
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
	// MaxChunkBytes limits the input size of each independent RaptorQ generation.
	// Zero defaults to 32 KiB.
	MaxChunkBytes int
	// AckTimeout is the time to wait for a chunk or message-completion
	// acknowledgment after sending a source-symbol stream or repair batch.
	// Zero defaults to 3 seconds.
	AckTimeout time.Duration
	// MaxRetries is the maximum number of repair-symbol batches per chunk after
	// the initial source-symbol stream. Zero defaults to 5.
	MaxRetries int
	// ReceiveTimeout is the maximum idle time before an incomplete incoming
	// message is discarded. Zero defaults to 2 minutes.
	ReceiveTimeout time.Duration
	// ReplayWindow is the retention period for completed message IDs used for
	// deduplication. Zero defaults to 10 minutes.
	ReplayWindow time.Duration
}

// DefaultConfig returns the baseline settings for a transparent serial radio
// link. It assumes the radio accepts wire frames up to 255 bytes; callers must
// adjust this and the timeouts for their module and air settings. Set Key before
// passing the config to Open or New.
func DefaultConfig() Config {
	return Config{
		MaxWireFrameBytes:  255,
		MaxAppMessageBytes: 1 << 20,
		MaxChunkBytes:      32 << 10,
		AckTimeout:         3 * time.Second,
		MaxRetries:         5,
		ReceiveTimeout:     2 * time.Minute,
		ReplayWindow:       10 * time.Minute,
	}
}

// normalized fills zero-valued options from DefaultConfig and validates limits.
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
	if c.MaxChunkBytes == 0 {
		c.MaxChunkBytes = defaults.MaxChunkBytes
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
		c.MaxChunkBytes < 1 || uint64(c.MaxChunkBytes) > uint64(^uint32(0)) ||
		c.AckTimeout < 0 || c.MaxRetries < 0 || c.ReceiveTimeout < 0 || c.ReplayWindow < 0 {
		return c, errors.New("invalid link configuration")
	}
	return c, nil
}

// Link owns its stream. Close must unblock concurrent Read and Write calls.
// One outgoing and one incoming message may be in progress at a time.
type Link struct {
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
// The returned Link owns the port and closes it when Link.Close is called.
func Open(port string, baudRate int, cfg Config) (*Link, error) {
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

// New starts a link over a transparent byte stream and takes ownership of it.
// The stream's Close method must unblock pending reads and writes.
func New(stream io.ReadWriteCloser, cfg Config) (*Link, error) {
	if stream == nil {
		return nil, errors.New("nil stream")
	}
	cfg, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	key := memguard.NewBuffer(len(cfg.Key))
	if key.Size() != len(cfg.Key) {
		return nil, errors.New("allocate protected key memory")
	}
	copy(key.Bytes(), cfg.Key)
	key.Freeze()
	cfg.Key = nil
	l := &Link{stream: stream, cfg: cfg, sendMu: make(chan struct{}, 1),
		key: key, acks: make(chan frame, 1), messages: make(chan []byte, 1), done: make(chan struct{})}
	if _, err := rand.Read(l.origin[:]); err != nil {
		key.Destroy()
		return nil, fmt.Errorf("generate link origin: %w", err)
	}
	l.sendMu <- struct{}{}
	go l.readLoop()
	return l, nil
}

// Close closes the underlying stream and unblocks pending operations.
func (l *Link) Close() error {
	l.fail(io.ErrClosedPipe)
	if err := l.failure(); err != io.ErrClosedPipe {
		return err
	}
	return nil
}

// fail records the first terminal error and closes the stream exactly once.
func (l *Link) fail(err error) {
	l.once.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
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

// failure returns the recorded terminal error under the link mutex.
func (l *Link) failure() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// write serializes a frame and writes it without interleaving other frames.
func (l *Link) write(f frame) error {
	select {
	case <-l.done:
		return l.failure()
	default:
	}
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

// parse decodes and authenticates a wire frame with the link's protected key.
func (l *Link) parse(encoded []byte) (frame, error) {
	l.keyMu.RLock()
	defer l.keyMu.RUnlock()
	if l.key == nil || !l.key.IsAlive() {
		return frame{}, errors.New("protected key is unavailable")
	}
	return parseFrame(encoded, l.key.Bytes())
}

// symbolSize returns the largest symbol payload that fits in one configured
// wire frame after headers, authentication, COBS encoding, and delimiter.
func (l *Link) symbolSize() int {
	// Reserve the worst-case COBS overhead, including the zero delimiter.
	return l.cfg.MaxWireFrameBytes - headerSize - macSize - l.cfg.MaxWireFrameBytes/254 - 2
}

// Send splits data into bounded RaptorQ generations as needed. It returns nil
// only after the receiver has reassembled and queued the whole message and the
// sender has received its message-completion acknowledgment.
func (l *Link) Send(ctx context.Context, data []byte) error {
	if len(data) == 0 || len(data) > l.cfg.MaxAppMessageBytes {
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
	chunkCapacity := l.chunkCapacity(uint16(size))
	chunkCount := uint32((uint64(len(data)) + uint64(chunkCapacity) - 1) / uint64(chunkCapacity))
	f := frame{kind: frameData, messageLen: uint32(len(data)), chunkCount: chunkCount, symbolSize: uint16(size)}
	if _, err := rand.Read(f.id[:]); err != nil {
		return fmt.Errorf("generate message ID: %w", err)
	}
	copy(f.id[:len(l.origin)], l.origin[:])
	for chunkIndex, start := uint32(0), 0; chunkIndex < chunkCount; chunkIndex++ {
		end := start + int(chunkCapacity)
		if end > len(data) {
			end = len(data)
		}
		chunk := data[start:end]
		enc, err := raptorq.NewRaptorQ(uint32(size)).CreateEncoder(chunk)
		if err != nil {
			return fmt.Errorf("create RaptorQ encoder for chunk %d: %w", chunkIndex, err)
		}
		f.chunkLen = uint32(len(chunk))
		f.chunkIndex = chunkIndex
		f.payload = nil
		if done, err := l.sendChunk(ctx, f, enc); err != nil {
			return err
		} else if done && chunkIndex+1 == chunkCount {
			return nil
		}
		start = end
	}
	return nil
}

// chunkCapacity bounds each RaptorQ generation by both configuration and the
// source-symbol limit for the selected symbol size.
func (l *Link) chunkCapacity(symbolSize uint16) uint32 {
	capacity := uint64(l.cfg.MaxChunkBytes)
	maxBySymbols := uint64(maxSourceSymbols) * uint64(symbolSize)
	if capacity > maxBySymbols {
		capacity = maxBySymbols
	}
	return uint32(capacity)
}

// sendChunk streams one RaptorQ generation and waits for its chunk or final ACK.
func (l *Link) sendChunk(ctx context.Context, f frame, enc *raptorq.Encoder) (bool, error) {
	sourceSymbols := enc.BaseSymbolsNum()
	// Stream all source symbols without waiting for per-symbol acknowledgments.
	for f.index = 0; f.index < sourceSymbols; f.index++ {
		if acked, err := l.checkChunkAck(ctx, f); err != nil {
			return false, err
		} else if acked {
			return true, nil
		}
		f.payload = enc.GenSymbol(f.index)
		if err := l.write(f); err != nil {
			return false, fmt.Errorf("send source symbol %d of chunk %d: %w", f.index, f.chunkIndex, err)
		}
		if acked, err := l.checkChunkAck(ctx, f); err != nil {
			return false, err
		} else if acked {
			return true, nil
		}
	}

	if acked, err := l.waitChunkAck(ctx, f); err != nil {
		return false, err
	} else if acked {
		return true, nil
	}

	// Add repair symbols in progressively larger batches until decoding is
	// acknowledged or the retry budget is exhausted.
	nextRepair := sourceSymbols
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
			if acked, err := l.checkChunkAck(ctx, f); err != nil {
				return false, err
			} else if acked {
				return true, nil
			}
			f.payload = enc.GenSymbol(f.index)
			if err := l.write(f); err != nil {
				return false, fmt.Errorf("send repair symbol %d of chunk %d: %w", f.index, f.chunkIndex, err)
			}
			if acked, err := l.checkChunkAck(ctx, f); err != nil {
				return false, err
			} else if acked {
				return true, nil
			}
		}
		nextRepair = batchEnd
		if acked, err := l.waitChunkAck(ctx, f); err != nil {
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

// expectedAckKind selects a chunk ACK or the final message-completion ACK.
func expectedAckKind(expected frame) byte {
	if expected.chunkIndex+1 == expected.chunkCount {
		return frameAck
	}
	return frameChunkAck
}

// checkChunkAck consumes a queued ACK and matches it to the expected chunk.
func (l *Link) checkChunkAck(ctx context.Context, expected frame) (bool, error) {
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-l.done:
			return false, l.failure()
		case ack := <-l.acks:
			if isChunkAck(ack, expected) {
				return true, nil
			}
		default:
			return false, nil
		}
	}
}

// waitChunkAck waits up to AckTimeout for an ACK matching the expected chunk.
func (l *Link) waitChunkAck(ctx context.Context, expected frame) (bool, error) {
	timer := time.NewTimer(l.cfg.AckTimeout)
	defer timer.Stop()
	for {
		if acked, err := l.checkChunkAck(ctx, expected); acked || err != nil {
			return acked, err
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-l.done:
			return false, l.failure()
		case ack := <-l.acks:
			if isChunkAck(ack, expected) {
				return true, nil
			}
		case <-timer.C:
			return false, nil
		}
	}
}

// isChunkAck reports whether ack acknowledges the expected RaptorQ generation.
func isChunkAck(ack, expected frame) bool {
	return ack.id == expected.id && ack.messageLen == expected.messageLen &&
		ack.chunkLen == expected.chunkLen && ack.chunkIndex == expected.chunkIndex &&
		ack.chunkCount == expected.chunkCount && ack.symbolSize == expected.symbolSize &&
		ack.kind == expectedAckKind(expected)
}

// Receive waits for the next completely reassembled message.
func (l *Link) Receive(ctx context.Context) ([]byte, error) {
	// Prefer a queued message even when the link has already closed.
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
