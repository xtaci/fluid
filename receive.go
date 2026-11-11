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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/xssnick/raptorq"
)

// incoming holds the state for the message currently being reassembled.
type incoming struct {
	// id identifies the message.
	id [16]byte
	// messageLen and chunkCount describe the complete application message.
	messageLen uint32
	chunkCount uint32
	// nextChunk is the next RaptorQ generation expected from the sender.
	nextChunk uint32
	// assembled contains all decoded chunks in order.
	assembled []byte
	// chunkLen and decoder describe the active RaptorQ generation.
	chunkLen uint32
	decoder  *raptorq.Decoder
	// deliveryReady means the final chunk is decoded but the application queue
	// was full when delivery was attempted.
	deliveryReady bool
	// symbolSize is the symbol size shared by this message's generations.
	symbolSize uint16
	// updated is the time the most recent symbol or retry was received.
	updated time.Time
}

// completed stores metadata for delivered messages so late symbols can trigger
// another completion acknowledgment without duplicate delivery.
type completed struct {
	// messageLen and symbolSize identify the completed message's shape.
	messageLen uint32
	chunkCount uint32
	symbolSize uint16
	// expires is when this deduplication entry can be discarded.
	expires time.Time
}

// readLoop extracts delimited frames, authenticates them, and dispatches them.
// Oversized frames are discarded through the next delimiter for resynchronization.
func (l *Link) readLoop() {
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
					if (f.kind == frameAck || f.kind == frameChunkAck) && len(f.payload) == 0 {
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

// handleData validates an authenticated symbol, decodes its generation, and
// acknowledges the generation or the complete message after queueing.
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
	if f.messageLen == 0 || uint64(f.messageLen) > uint64(l.cfg.MaxAppMessageBytes) ||
		f.symbolSize == 0 || int(f.symbolSize) > l.symbolSize() || len(f.payload) != int(f.symbolSize) {
		return nil
	}
	chunkCapacity := uint64(l.chunkCapacity(f.symbolSize))
	expectedChunks := (uint64(f.messageLen) + chunkCapacity - 1) / chunkCapacity
	chunkStart := uint64(f.chunkIndex) * chunkCapacity
	if f.chunkCount == 0 || uint64(f.chunkCount) != expectedChunks ||
		f.chunkIndex >= f.chunkCount || chunkStart >= uint64(f.messageLen) {
		return nil
	}
	expectedChunkLen := chunkCapacity
	if remaining := uint64(f.messageLen) - chunkStart; expectedChunkLen > remaining {
		expectedChunkLen = remaining
	}
	if uint64(f.chunkLen) != expectedChunkLen {
		return nil
	}
	sourceSymbols := (uint64(f.chunkLen) + uint64(f.symbolSize) - 1) / uint64(f.symbolSize)
	if sourceSymbols > maxSourceSymbols || !validSymbolID(f.index) {
		return nil
	}
	// Acknowledge retries for completed messages without delivering them again.
	if entry, ok := completedIDs[f.id]; ok {
		if entry.messageLen == f.messageLen && entry.chunkCount == f.chunkCount &&
			entry.symbolSize == f.symbolSize {
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
		*current = &incoming{id: f.id, messageLen: f.messageLen, chunkCount: f.chunkCount,
			symbolSize: f.symbolSize, updated: now}
	}
	s := *current
	if s.id != f.id || s.messageLen != f.messageLen || s.chunkCount != f.chunkCount ||
		s.symbolSize != f.symbolSize {
		return nil
	}
	s.updated = now
	// Completed chunks may be retransmitted when their chunk ACK was lost.
	if f.chunkIndex < s.nextChunk {
		if s.deliveryReady && f.chunkIndex+1 == s.chunkCount {
			return l.deliverIncoming(s, current, completedIDs, f, now)
		}
		return l.ack(f, frameChunkAck)
	}
	if f.chunkIndex != s.nextChunk {
		return nil
	}
	if s.deliveryReady {
		return l.deliverIncoming(s, current, completedIDs, f, now)
	}
	if s.decoder == nil {
		dec, err := raptorq.NewRaptorQ(uint32(f.symbolSize)).CreateDecoder(f.chunkLen)
		if err != nil {
			return fmt.Errorf("create RaptorQ decoder: %w", err)
		}
		s.chunkLen = f.chunkLen
		s.decoder = dec
	}
	if s.chunkLen != f.chunkLen {
		return nil
	}
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
	if uint64(len(msg)) != uint64(s.chunkLen) {
		return errors.New("RaptorQ decoded unexpected chunk length")
	}
	s.assembled = append(s.assembled, msg...)
	s.nextChunk++
	s.decoder = nil
	s.chunkLen = 0
	if s.nextChunk < s.chunkCount {
		return l.ack(f, frameChunkAck)
	}
	if uint64(len(s.assembled)) != uint64(s.messageLen) {
		return errors.New("reassembled message length mismatch")
	}
	s.deliveryReady = true
	return l.deliverIncoming(s, current, completedIDs, f, now)
}

// deliverIncoming queues a fully reassembled message and sends its final ACK.
func (l *Link) deliverIncoming(s *incoming, current **incoming, completedIDs map[[16]byte]completed, f frame, now time.Time) error {
	select {
	case l.messages <- s.assembled:
		completedIDs[s.id] = completed{s.messageLen, s.chunkCount, s.symbolSize, now.Add(l.cfg.ReplayWindow)}
		*current = nil
		return l.ack(f, frameAck)
	default:
		return nil
	}
}

// ack sends an authenticated chunk or message-completion acknowledgment.
func (l *Link) ack(f frame, kind byte) error {
	f.kind = kind
	f.payload = nil
	return l.write(f)
}
