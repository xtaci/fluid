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
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/xssnick/raptorq"
)

type incoming struct {
	id         [16]byte
	messageLen uint32
	symbolSize uint16
	next       uint32
	total      uint32
	decoder    *raptorq.Decoder
	updated    time.Time
}

type completed struct {
	messageLen uint32
	symbolSize uint16
	total      uint32
	expires    time.Time
}

// readLoop extracts delimited frames, authenticates them, and dispatches them.
func (l *Link) readLoop() {
	var active *incoming
	completedIDs := make(map[[16]byte]completed)
	buf := make([]byte, 1024)
	packet := make([]byte, 0, l.cfg.MaxFrameSize)
	overflow := false
	for {
		n, err := l.stream.Read(buf)
		// Discard an oversized packet until the next delimiter to resynchronize.
		for _, b := range buf[:n] {
			if b != 0 {
				if len(packet) >= l.cfg.MaxFrameSize-1 {
					overflow = true
				} else if !overflow {
					packet = append(packet, b)
				}
				continue
			}
			// Only authenticated frames can affect acknowledgment or receive state.
			if !overflow && len(packet) > 0 {
				f, parseErr := parseFrame(packet, l.cfg.Key)
				if parseErr == nil {
					if f.kind == frameAck && len(f.payload) == 0 {
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

// handleData validates an authenticated symbol, updates reassembly, and
// acknowledges it only after the symbol or completed message is accepted.
func (l *Link) handleData(f frame, current **incoming, completedIDs map[[16]byte]completed) error {
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
	if f.messageLen == 0 || uint64(f.messageLen) > uint64(l.cfg.MaxMessageSize) ||
		f.symbolSize == 0 || int(f.symbolSize) > l.symbolSize() || len(f.payload) != int(f.symbolSize) {
		return nil
	}
	total := (uint64(f.messageLen) + uint64(f.symbolSize) - 1) / uint64(f.symbolSize)
	if total > maxSymbols || uint64(f.index) >= total {
		return nil
	}
	// Acknowledge retries for completed messages without delivering them again.
	if entry, ok := completedIDs[f.id]; ok {
		if entry.messageLen == f.messageLen && entry.symbolSize == f.symbolSize && f.index < entry.total {
			return l.ack(f)
		}
		return nil
	}
	// Accept a new message only after the active receive state expires.
	if *current != nil && now.Sub((*current).updated) >= l.cfg.ReceiveTimeout {
		*current = nil
	}
	if *current == nil {
		// Do not evict unexpired IDs: doing so could deliver a retry twice.
		if f.index != 0 || len(completedIDs) >= 128 {
			return nil
		}
		dec, err := raptorq.NewRaptorQ(uint32(f.symbolSize)).CreateDecoder(f.messageLen)
		if err != nil {
			return fmt.Errorf("create RaptorQ decoder: %w", err)
		}
		*current = &incoming{id: f.id, messageLen: f.messageLen, symbolSize: f.symbolSize,
			total: uint32(total), decoder: dec, updated: now}
	}
	// Stop-and-wait accepts the next symbol or acknowledges an earlier retry.
	s := *current
	if s.id != f.id || s.messageLen != f.messageLen || s.symbolSize != f.symbolSize || f.index > s.next {
		return nil
	}
	s.updated = now
	if f.index < s.next {
		return l.ack(f)
	}
	ready, err := s.decoder.AddSymbol(f.index, f.payload)
	if err != nil {
		return fmt.Errorf("add RaptorQ symbol: %w", err)
	}
	// The final acknowledgment is sent only after successful queueing.
	if ready {
		ok, msg, err := s.decoder.Decode()
		if err != nil {
			return fmt.Errorf("decode RaptorQ message: %w", err)
		}
		if !ok {
			return fmt.Errorf("decode RaptorQ message: insufficient symbols")
		}
		select {
		case l.messages <- msg:
			completedIDs[f.id] = completed{f.messageLen, f.symbolSize, s.total, now.Add(l.cfg.ReplayWindow)}
			*current = nil
		default:
			// Keep the final symbol unacknowledged until the application drains
			// the queue; the sender's next retry will try delivery again.
			return nil
		}
	} else {
		s.next++
	}
	return l.ack(f)
}

// ack sends an authenticated acknowledgment bound to the data frame metadata.
func (l *Link) ack(f frame) error {
	f.kind = frameAck
	f.payload = nil
	return l.write(f)
}
