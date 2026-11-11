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

// incoming holds the state for the message currently being reassembled.
type incoming struct {
	// id identifies the message.
	id [16]byte
	// messageLen and symbolSize describe the encoded message and its symbols.
	messageLen uint32
	symbolSize uint16
	// decoder accumulates symbols and reconstructs the message.
	decoder *raptorq.Decoder
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
				f, parseErr := l.parse(packet)
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
// acknowledges only after the complete message is queued.
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
	sourceSymbols := (uint64(f.messageLen) + uint64(f.symbolSize) - 1) / uint64(f.symbolSize)
	if sourceSymbols > maxSourceSymbols || !validSymbolID(f.index) {
		return nil
	}
	// Acknowledge retries for completed messages without delivering them again.
	if entry, ok := completedIDs[f.id]; ok {
		if entry.messageLen == f.messageLen && entry.symbolSize == f.symbolSize {
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
		if len(completedIDs) >= 128 {
			return nil
		}
		dec, err := raptorq.NewRaptorQ(uint32(f.symbolSize)).CreateDecoder(f.messageLen)
		if err != nil {
			return fmt.Errorf("create RaptorQ decoder: %w", err)
		}
		*current = &incoming{id: f.id, messageLen: f.messageLen, symbolSize: f.symbolSize,
			decoder: dec, updated: now}
	}
	s := *current
	if s.id != f.id || s.messageLen != f.messageLen || s.symbolSize != f.symbolSize {
		return nil
	}
	s.updated = now
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
	select {
	case l.messages <- msg:
		completedIDs[f.id] = completed{f.messageLen, f.symbolSize, now.Add(l.cfg.ReplayWindow)}
		*current = nil
		return l.ack(f)
	default:
		// Leave the message unacknowledged until the application drains the
		// queue; the sender's next repair symbol will retry delivery.
		return nil
	}
}

// ack sends an authenticated message-completion acknowledgment.
func (l *Link) ack(f frame) error {
	f.kind = frameAck
	f.payload = nil
	return l.write(f)
}
