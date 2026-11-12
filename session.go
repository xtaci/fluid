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

package fluid

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"
)

const (
	sessionMagic      = "FDX1"
	sessionHeaderSize = 42
	sessionPoll       = 1
	sessionReply      = 2
)

// SessionConfig configures a single byte stream over an exclusively owned Message.
// Exactly one endpoint must set Primary. The other endpoint responds to polls.
type SessionConfig struct {
	Primary bool
	// PollInterval is the maximum idle time between polls, excluding replay
	// pacing, send time and retransmissions. Zero defaults to five seconds.
	PollInterval time.Duration
	// ExchangeTimeout bounds each turn, excluding replay pacing. Zero defaults
	// to two minutes. It must exceed PollInterval.
	ExchangeTimeout time.Duration
	// BufferBytes bounds the buffered bytes in each direction. Zero defaults
	// to 16 KiB.
	BufferBytes int
	// BlockBytes bounds payload per turn. Zero defaults to 512 bytes.
	BlockBytes int
}

// DefaultSessionConfig returns conservative defaults; tune them for the radio.
func DefaultSessionConfig() SessionConfig {
	return SessionConfig{
		PollInterval:    5 * time.Second,
		ExchangeTimeout: 2 * time.Minute,
		BufferBytes:     16 << 10,
		BlockBytes:      512,
	}
}

// Session is a bounded, ordered bidirectional byte stream. It exclusively owns
// its Message. Read and Write may run concurrently, but neither a second
// Session nor direct Message operations may use the same Message.
//
// Write reports bytes accepted into local memory, not delivery to the peer.
// Close aborts the connection and can discard buffered bytes. Use Flush to
// wait until the peer Message has queued preceding writes. Neither Flush nor
// Read implies durable application storage.
type Session struct {
	message *Message
	cfg     SessionConfig
	ctx     context.Context
	cancel  context.CancelFunc

	mu        sync.Mutex
	changed   chan struct{}
	err       error
	in        []byte
	out       []byte
	accepted  uint64
	delivered uint64
	done      chan struct{}
}

// NewSession starts a session stream. Both peers must use this protocol, and the
// secondary must be started before the primary's exchange timeout expires.
// Message is closed when the stream fails or is closed.
func NewSession(message *Message, cfg SessionConfig) (*Session, error) {
	if message == nil {
		return nil, errors.New("nil message")
	}
	defaults := DefaultSessionConfig()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaults.PollInterval
	}
	if cfg.ExchangeTimeout == 0 {
		cfg.ExchangeTimeout = defaults.ExchangeTimeout
	}
	if cfg.BufferBytes == 0 {
		cfg.BufferBytes = defaults.BufferBytes
	}
	if cfg.BlockBytes == 0 {
		cfg.BlockBytes = defaults.BlockBytes
	}
	if cfg.PollInterval < 0 || cfg.ExchangeTimeout <= cfg.PollInterval ||
		cfg.BufferBytes < 1 || uint64(cfg.BufferBytes) > uint64(^uint32(0)) ||
		cfg.BlockBytes < 1 || cfg.BlockBytes > cfg.BufferBytes ||
		cfg.BlockBytes > message.cfg.MaxAppMessageBytes-sessionHeaderSize ||
		uint64(cfg.BlockBytes)+sessionHeaderSize > uint64(maxSourceSymbols)*uint64(message.symbolSize()) ||
		cfg.ExchangeTimeout > time.Duration(math.MaxInt64)-message.cfg.ReplayWindow {
		return nil, errors.New("invalid session configuration")
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &Session{message: message, cfg: cfg, ctx: ctx, cancel: cancel, changed: make(chan struct{}), done: make(chan struct{})}
	go d.run()
	return d, nil
}

func (d *Session) notify() {
	close(d.changed)
	d.changed = make(chan struct{})
}

func (d *Session) fail(err error) {
	d.mu.Lock()
	if d.err == nil {
		d.err = err
		d.notify()
	}
	d.mu.Unlock()
	d.cancel()
}

func (d *Session) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for len(d.in) == 0 {
		if d.err != nil {
			return 0, d.err
		}
		wait := d.changed
		d.mu.Unlock()
		<-wait
		d.mu.Lock()
	}
	n := copy(p, d.in)
	d.in = d.in[n:]
	d.notify()
	return n, nil
}

func (d *Session) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for len(p) > 0 {
		if d.err != nil {
			return n, d.err
		}
		space := d.cfg.BufferBytes - len(d.out)
		if space == 0 {
			wait := d.changed
			d.mu.Unlock()
			<-wait
			d.mu.Lock()
			continue
		}
		count := min(space, len(p))
		if uint64(count) > ^uint64(0)-d.accepted {
			return n, errors.New("session write counter overflow")
		}
		d.out = append(d.out, p[:count]...)
		d.accepted += uint64(count)
		n += count
		p = p[count:]
		d.notify()
	}
	return n, nil
}

// Flush waits until all writes accepted before the call have been queued by the
// peer Message. This does not guarantee that the peer application has read them.
// Cancellation stops waiting but does not cancel the stream.
func (d *Session) Flush(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	target := d.accepted
	for d.delivered < target {
		if d.err != nil {
			return d.err
		}
		wait := d.changed
		d.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			d.mu.Lock()
			return ctx.Err()
		}
		d.mu.Lock()
	}
	return nil
}

// Close aborts the stream and closes its underlying Message.
func (d *Session) Close() error {
	d.fail(io.ErrClosedPipe)
	err := d.message.Close()
	<-d.done
	return err
}

type sessionPacket struct {
	kind   byte
	id     [16]byte
	seq    uint64
	credit uint32
	more   bool
	replay time.Duration
	data   []byte
}

func encodeSession(p sessionPacket) []byte {
	out := make([]byte, sessionHeaderSize+len(p.data))
	copy(out, sessionMagic)
	out[4] = p.kind
	copy(out[5:21], p.id[:])
	binary.BigEndian.PutUint64(out[21:29], p.seq)
	binary.BigEndian.PutUint32(out[29:33], p.credit)
	if p.more {
		out[33] = 1
	}
	binary.BigEndian.PutUint64(out[34:42], uint64(p.replay))
	copy(out[sessionHeaderSize:], p.data)
	return out
}

func decodeSession(data []byte) (sessionPacket, error) {
	var p sessionPacket
	if len(data) < sessionHeaderSize || string(data[:4]) != sessionMagic ||
		(data[4] != sessionPoll && data[4] != sessionReply) || data[33] > 1 {
		return p, errors.New("invalid session packet")
	}
	p.kind = data[4]
	copy(p.id[:], data[5:21])
	p.seq = binary.BigEndian.Uint64(data[21:29])
	p.credit = binary.BigEndian.Uint32(data[29:33])
	p.more = data[33] == 1
	p.replay = time.Duration(binary.BigEndian.Uint64(data[34:42]))
	p.data = data[sessionHeaderSize:]
	if p.replay <= 0 {
		return p, errors.New("invalid peer replay window")
	}
	return p, nil
}

// A Message receiver retains 128 IDs. This pacing is conservative and assumes the
// Message is fresh and reserved exclusively for this session session.
type sessionPacer struct {
	sent [128]time.Time
	next uint64
}

func (p *sessionPacer) wait(ctx context.Context, window time.Duration) error {
	if window <= 0 || p.next < uint64(len(p.sent)) {
		return nil
	}
	delay := time.Until(p.sent[p.next%uint64(len(p.sent))].Add(window))
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *sessionPacer) record() {
	p.sent[p.next%uint64(len(p.sent))] = time.Now()
	p.next++
}

func (d *Session) packet(kind byte, id [16]byte, seq uint64, credit uint32) sessionPacket {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := min(len(d.out), d.cfg.BlockBytes)
	if uint64(n) > uint64(credit) {
		n = int(credit)
	}
	return sessionPacket{kind: kind, id: id, seq: seq,
		credit: uint32(d.cfg.BufferBytes - len(d.in)), more: len(d.out) > n,
		replay: d.message.cfg.ReplayWindow, data: append([]byte(nil), d.out[:n]...)}
}

func (d *Session) accept(data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(data) > d.cfg.BufferBytes-len(d.in) {
		return errors.New("peer exceeded advertised receive window")
	}
	d.in = append(d.in, data...)
	if len(data) > 0 {
		d.notify()
	}
	return nil
}

func (d *Session) committed(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.out = d.out[n:]
	d.delivered += uint64(n)
	if n > 0 {
		d.notify()
	}
}

func (d *Session) send(ctx context.Context, pacer *sessionPacer, peerReplay time.Duration, p sessionPacket) error {
	if err := pacer.wait(ctx, peerReplay); err != nil {
		return err
	}
	if err := d.message.Send(ctx, encodeSession(p)); err != nil {
		return err
	}
	pacer.record()
	return nil
}

func (d *Session) run() {
	defer close(d.done)
	err := d.exchange()
	if err != nil && d.ctx.Err() == nil {
		d.fail(err)
	}
	if err := d.message.Close(); err != nil {
		d.fail(fmt.Errorf("close session message: %w", err))
	}
}

func (d *Session) exchange() error {
	var id [16]byte
	if d.cfg.Primary {
		if _, err := rand.Read(id[:]); err != nil {
			return fmt.Errorf("generate session session ID: %w", err)
		}
	}
	var seq uint64
	var credit uint32
	var peerReplay time.Duration
	var pacer sessionPacer
	for {
		timeout := d.cfg.ExchangeTimeout
		if seq >= uint64(len(pacer.sent)) {
			window := max(peerReplay, d.message.cfg.ReplayWindow)
			if window > time.Duration(math.MaxInt64)-timeout {
				return errors.New("peer replay window exceeds supported timeout")
			}
			timeout += window
		}
		ctx, cancel := context.WithTimeout(d.ctx, timeout)
		if d.cfg.Primary {
			p := d.packet(sessionPoll, id, seq, credit)
			err := d.send(ctx, &pacer, peerReplay, p)
			if err != nil {
				cancel()
				return fmt.Errorf("send session poll: %w", err)
			}
			msg, err := d.message.Receive(ctx)
			cancel()
			if err != nil {
				return fmt.Errorf("receive session reply: %w", err)
			}
			reply, err := decodeSession(msg)
			if err != nil || reply.kind != sessionReply || reply.id != id || reply.seq != seq {
				return errors.New("invalid session reply or sequence")
			}
			if err := d.accept(reply.data); err != nil {
				return err
			}
			d.committed(len(p.data))
			credit, peerReplay = reply.credit, reply.replay
			seq++
			d.mu.Lock()
			immediate := (len(d.out) > 0 && credit > 0) || (reply.more && len(d.in) < d.cfg.BufferBytes)
			wait := d.changed
			d.mu.Unlock()
			if !immediate {
				timer := time.NewTimer(d.cfg.PollInterval)
				select {
				case <-timer.C:
				case <-wait:
				case <-d.ctx.Done():
					timer.Stop()
					return d.ctx.Err()
				}
				timer.Stop()
			}
		} else {
			msg, err := d.message.Receive(ctx)
			if err != nil {
				cancel()
				return fmt.Errorf("receive session poll: %w", err)
			}
			poll, err := decodeSession(msg)
			if err != nil || poll.kind != sessionPoll || poll.seq != seq || (seq != 0 && poll.id != id) {
				cancel()
				return errors.New("invalid session poll or sequence")
			}
			if seq == 0 {
				id = poll.id
			}
			if err := d.accept(poll.data); err != nil {
				cancel()
				return err
			}
			reply := d.packet(sessionReply, id, seq, poll.credit)
			err = d.send(ctx, &pacer, poll.replay, reply)
			cancel()
			if err != nil {
				return fmt.Errorf("send session reply: %w", err)
			}
			d.committed(len(reply.data))
			seq++
		}
	}
}
