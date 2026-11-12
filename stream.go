// Copyright (c) 2026 xtaci
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do do so, subject to the following conditions:
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
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	streamMagic      = "FST1"
	streamHeaderSize = 29
	streamData       = 1
	streamEnd        = 2
	streamAck        = 3
	streamEndSize    = streamHeaderSize + 8 + sha256.Size
)

func streamHeader(kind byte, id [16]byte, seq uint64) []byte {
	header := make([]byte, streamHeaderSize)
	copy(header, streamMagic)
	header[4] = kind
	copy(header[5:21], id[:])
	binary.BigEndian.PutUint64(header[21:], seq)
	return header
}

func parseStreamHeader(msg []byte) (byte, [16]byte, uint64, error) {
	var id [16]byte
	if len(msg) < streamHeaderSize || string(msg[:4]) != streamMagic {
		return 0, id, 0, errors.New("invalid stream message header")
	}
	copy(id[:], msg[5:21])
	return msg[4], id, binary.BigEndian.Uint64(msg[21:29]), nil
}

// SendStream sends bytes read from src as ordered Link messages. blockSize is
// the maximum data bytes per message, excluding protocol headers. It returns
// only after the receiver verifies the complete SHA-256 digest and confirms
// that its writer accepted the data. Both directions of the Link must be
// reserved for this transfer until it finishes. On failure, the receiver may
// have written some or all of the data. Cancel or close the peer separately.
// The context cannot interrupt a blocked src.Read or underlying stream.Write.
func (l *Link) SendStream(ctx context.Context, src io.Reader, blockSize int) (uint64, error) {
	if src == nil {
		return 0, errors.New("nil stream source")
	}
	if blockSize < 1 || blockSize > l.cfg.MaxAppMessageBytes-streamHeaderSize || l.cfg.MaxAppMessageBytes < streamEndSize {
		return 0, errors.New("invalid stream block size or maximum message size")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return 0, fmt.Errorf("generate stream ID: %w", err)
	}
	hash := sha256.New()
	buf := make([]byte, blockSize)
	var count, seq uint64
	// A receiving Link retains up to 128 message IDs for its replay window.
	// Wait until our oldest acknowledged message must have expired before
	// sending the next one; acknowledge time is later than insertion time.
	var sent [128]time.Time
	send := func(msg []byte) error {
		slot := seq % uint64(len(sent))
		if !sent[slot].IsZero() {
			wait := time.Until(sent[slot].Add(l.cfg.ReplayWindow))
			if wait > 0 {
				timer := time.NewTimer(wait)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					return ctx.Err()
				case <-l.done:
					return l.failure()
				}
			}
		}
		if err := l.Send(ctx, msg); err != nil {
			return err
		}
		sent[slot] = time.Now()
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		n, readErr := io.ReadFull(src, buf)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return count, fmt.Errorf("read stream source: %w", readErr)
		}
		if n > 0 {
			if uint64(n) > ^uint64(0)-count || seq == ^uint64(0) {
				return count, errors.New("stream size or sequence overflow")
			}
			msg := append(streamHeader(streamData, id, seq), buf[:n]...)
			if err := send(msg); err != nil {
				return count, fmt.Errorf("send stream block %d: %w", seq, err)
			}
			hash.Write(buf[:n])
			count += uint64(n)
			seq++
		}
		if readErr != nil {
			break
		}
	}
	end := streamHeader(streamEnd, id, seq)
	end = binary.BigEndian.AppendUint64(end, count)
	end = append(end, hash.Sum(nil)...)
	if err := send(end); err != nil {
		return count, fmt.Errorf("send stream end: %w", err)
	}
	ack, err := l.Receive(ctx)
	if err != nil {
		return count, fmt.Errorf("receive stream confirmation: %w", err)
	}
	expected := append(streamHeader(streamAck, id, seq), end[streamHeaderSize:]...)
	if !bytes.Equal(ack, expected) {
		return count, errors.New("invalid stream confirmation")
	}
	return count, nil
}

// ReceiveStream receives one SendStream transfer, writes its bytes to dst, and
// verifies the complete SHA-256 digest. maxBytes is a strict upper bound; zero
// permits only an empty stream. Both directions of the Link must be reserved
// until it finishes. On error, dst may contain partial or unverified data; use
// a temporary file and publish it only on success. A nil return means dst
// accepted all bytes, not that they were flushed to durable storage.
// The context cannot interrupt a blocked dst.Write or underlying stream.Write.
func (l *Link) ReceiveStream(ctx context.Context, dst io.Writer, maxBytes uint64) (uint64, error) {
	if dst == nil {
		return 0, errors.New("nil stream destination")
	}
	hash := sha256.New()
	var id [16]byte
	var count, seq uint64
	for {
		msg, err := l.Receive(ctx)
		if err != nil {
			return count, fmt.Errorf("receive stream block: %w", err)
		}
		kind, gotID, gotSeq, err := parseStreamHeader(msg)
		if err != nil {
			return count, err
		}
		if seq == 0 {
			id = gotID
		}
		if id != gotID || gotSeq != seq {
			return count, errors.New("unexpected stream ID or sequence")
		}
		switch kind {
		case streamData:
			data := msg[streamHeaderSize:]
			if len(data) == 0 || uint64(len(data)) > maxBytes-count || seq == ^uint64(0) {
				return count, errors.New("invalid stream block or stream exceeds maximum size")
			}
			n, err := dst.Write(data)
			if n < 0 || n > len(data) {
				return count, errors.New("invalid stream destination write count")
			}
			count += uint64(n)
			if err != nil {
				return count, fmt.Errorf("write stream destination: %w", err)
			}
			if n != len(data) {
				return count, io.ErrShortWrite
			}
			hash.Write(data)
			seq++
		case streamEnd:
			if len(msg) != streamEndSize || binary.BigEndian.Uint64(msg[streamHeaderSize:]) != count ||
				!bytes.Equal(msg[streamHeaderSize+8:], hash.Sum(nil)) {
				return count, errors.New("stream length or digest mismatch")
			}
			ack := append(streamHeader(streamAck, id, seq), msg[streamHeaderSize:]...)
			if err := l.Send(ctx, ack); err != nil {
				return count, fmt.Errorf("send stream confirmation: %w", err)
			}
			return count, nil
		default:
			return count, errors.New("unexpected stream message type")
		}
	}
}
