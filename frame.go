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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	protocolVersion = 1
	frameData       = 1
	frameAck        = 2
	headerSize      = 28          // 1 version + 1 kind + 16 ID + 4 index + 4 message length + 2 symbol size
	macSize         = sha256.Size // HMAC-SHA256 length in bytes
)

type frame struct {
	kind       byte
	id         [16]byte
	index      uint32 // RaptorQ source or repair symbol ID.
	messageLen uint32
	symbolSize uint16
	payload    []byte
}

// Frame layout:
//
//	Raw frame (HMAC covers the header and payload):
//	+-----------+--------+------------+-------------+---------------+---------------+-----------+---------------+
//	| Version   | Kind   | ID (16)    | Symbol ID(4)| Msg len (4)   | Symbol (2)    | Payload   | HMAC-SHA256   |
//	+-----------+--------+------------+-------------+---------------+---------------+-----------+---------------+
//	| 1 byte    | 1      | 16 bytes   | 4 bytes     | 4 bytes       | 2 bytes       | variable  | 32 bytes      |
//
//	Wire frame: COBS(raw frame) + 0x00 delimiter
//
// marshalFrame authenticates each symbol payload and its metadata, then adds
// COBS framing.
func marshalFrame(f frame, key []byte) []byte {
	raw := make([]byte, headerSize+len(f.payload)+macSize)
	raw[0], raw[1] = protocolVersion, f.kind
	copy(raw[2:18], f.id[:])
	binary.BigEndian.PutUint32(raw[18:22], f.index)
	binary.BigEndian.PutUint32(raw[22:26], f.messageLen)
	binary.BigEndian.PutUint16(raw[26:28], f.symbolSize)
	copy(raw[headerSize:], f.payload)
	mac := hmac.New(sha256.New, key)
	mac.Write(raw[:len(raw)-macSize])
	copy(raw[len(raw)-macSize:], mac.Sum(nil))
	return append(cobsEncode(raw), 0)
}

// parseFrame decodes and authenticates a frame before exposing its fields.
func parseFrame(encoded, key []byte) (frame, error) {
	raw, err := cobsDecode(encoded)
	if err != nil || len(raw) < headerSize+macSize {
		return frame{}, errors.New("invalid frame encoding or length")
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(raw[:len(raw)-macSize])
	if !hmac.Equal(raw[len(raw)-macSize:], mac.Sum(nil)) {
		return frame{}, errors.New("invalid frame HMAC")
	}
	if raw[0] != protocolVersion || (raw[1] != frameData && raw[1] != frameAck) {
		return frame{}, errors.New("unsupported frame")
	}
	f := frame{
		kind:       raw[1],
		index:      binary.BigEndian.Uint32(raw[18:22]),
		messageLen: binary.BigEndian.Uint32(raw[22:26]),
		symbolSize: binary.BigEndian.Uint16(raw[26:28]),
		payload:    raw[headerSize : len(raw)-macSize],
	}
	copy(f.id[:], raw[2:18])
	return f, nil
}

// cobsEncode encodes src without zero bytes, leaving room for a frame delimiter.
func cobsEncode(src []byte) []byte {
	out := make([]byte, 1, len(src)+len(src)/254+1)
	codeAt, code := 0, byte(1)
	// Each code byte describes a run of nonzero bytes; 0xff starts another
	// run without implying an intervening zero.
	for _, b := range src {
		if b == 0 {
			out[codeAt] = code
			codeAt = len(out)
			out = append(out, 0)
			code = 1
		} else {
			out = append(out, b)
			code++
			if code == 0xff {
				out[codeAt] = code
				codeAt = len(out)
				out = append(out, 0)
				code = 1
			}
		}
	}
	out[codeAt] = code
	return out
}

// cobsDecode reverses COBS encoding and rejects truncated or invalid runs.
func cobsDecode(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, errors.New("invalid COBS")
	}
	out := make([]byte, 0, len(src))
	for i := 0; i < len(src); {
		code := int(src[i])
		if code == 0 || i+code > len(src) {
			return nil, errors.New("invalid COBS")
		}
		i++
		end := i + code - 1
		out = append(out, src[i:end]...)
		i = end
		// Short runs represent a zero unless they end the encoded frame.
		if code != 0xff && i < len(src) {
			out = append(out, 0)
		}
	}
	return out, nil
}
