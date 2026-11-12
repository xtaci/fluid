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

package ebyte

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xtaci/fluid"
	"go.bug.st/serial"
)

type symbolID struct {
	message [16]byte
	index   uint32
}

type frameCounts struct {
	mu             sync.Mutex
	frames         int
	wireBytes      int
	source, repair map[symbolID]struct{}
}

func newFrameCounts() *frameCounts {
	return &frameCounts{source: make(map[symbolID]struct{}), repair: make(map[symbolID]struct{})}
}

func (c *frameCounts) record(packet, key []byte) {
	raw, ok := decodeCOBS(packet)
	if !ok || len(raw) < 28+md5.Size {
		return
	}
	mac := hmac.New(md5.New, key)
	mac.Write(raw[:len(raw)-md5.Size])
	if !hmac.Equal(mac.Sum(nil), raw[len(raw)-md5.Size:]) || raw[0] != 1 || raw[1] != 1 {
		return
	}
	symbolSize := binary.BigEndian.Uint16(raw[26:28])
	if symbolSize == 0 {
		return
	}
	var id symbolID
	copy(id.message[:], raw[2:18])
	id.index = binary.BigEndian.Uint32(raw[18:22])
	messageLen := binary.BigEndian.Uint32(raw[22:26])
	sourceCount := (uint64(messageLen) + uint64(symbolSize) - 1) / uint64(symbolSize)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frames++
	c.wireBytes += len(packet) + 1
	if uint64(id.index) < sourceCount {
		c.source[id] = struct{}{}
	} else {
		c.repair[id] = struct{}{}
	}
}

func decodeCOBS(src []byte) ([]byte, bool) {
	var out []byte
	for len(src) > 0 {
		code := int(src[0])
		if code == 0 || code > len(src) {
			return nil, false
		}
		out = append(out, src[1:code]...)
		src = src[code:]
		if code < 255 && len(src) > 0 {
			out = append(out, 0)
		}
	}
	return out, true
}

type observedStream struct {
	io.ReadWriteCloser
	key             []byte
	read            *frameCounts
	written         *frameCounts
	readBuf         []byte
	writeBuf        []byte
	dropFirstSource bool
	injectedDrops   int
}

func observe(data []byte, pending *[]byte, counts *frameCounts, key []byte) {
	for _, b := range data {
		if b == 0 {
			if len(*pending) != 0 {
				counts.record(*pending, key)
			}
			*pending = (*pending)[:0]
		} else {
			*pending = append(*pending, b)
		}
	}
}

func (s *observedStream) Read(p []byte) (int, error) {
	n, err := s.ReadWriteCloser.Read(p)
	observe(p[:n], &s.readBuf, s.read, s.key)
	return n, err
}

func (s *observedStream) Write(p []byte) (int, error) {
	if s.dropFirstSource && len(p) > 1 && p[len(p)-1] == 0 {
		raw, ok := decodeCOBS(p[:len(p)-1])
		if ok && len(raw) >= 28+md5.Size && raw[0] == 1 && raw[1] == 1 &&
			binary.BigEndian.Uint32(raw[18:22]) == 0 {
			observe(p, &s.writeBuf, s.written, s.key)
			s.injectedDrops++
			return len(p), nil
		}
	}
	n, err := s.ReadWriteCloser.Write(p)
	observe(p[:n], &s.writeBuf, s.written, s.key)
	return n, err
}

func countMissing(sent, received map[symbolID]struct{}) int {
	missing := 0
	for id := range sent {
		if _, ok := received[id]; !ok {
			missing++
		}
	}
	return missing
}

func TestFrameObservation(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	counts := newFrameCounts()
	packet := func(index uint32) []byte {
		raw := make([]byte, 28+4+md5.Size)
		raw[0], raw[1] = 1, 1
		raw[2] = 9
		binary.BigEndian.PutUint32(raw[18:22], index)
		binary.BigEndian.PutUint32(raw[22:26], 7)
		binary.BigEndian.PutUint16(raw[26:28], 4)
		mac := hmac.New(md5.New, key)
		mac.Write(raw[:len(raw)-md5.Size])
		copy(raw[len(raw)-md5.Size:], mac.Sum(nil))
		encoded := []byte{0}
		codePos, code := 0, byte(1)
		for _, b := range raw {
			if b == 0 {
				encoded[codePos] = code
				codePos, code = len(encoded), 1
				encoded = append(encoded, 0)
			} else {
				encoded = append(encoded, b)
				code++
				if code == 255 {
					encoded[codePos] = code
					codePos, code = len(encoded), 1
					encoded = append(encoded, 0)
				}
			}
		}
		encoded[codePos] = code
		return encoded
	}
	counts.record(packet(0), key)
	counts.record(packet(2), key)
	bad := packet(1)
	bad[len(bad)-1] ^= 1
	counts.record(bad, key)
	counts.record([]byte{5, 1}, key)
	if counts.frames != 2 || len(counts.source) != 1 || len(counts.repair) != 1 {
		t.Fatalf("unexpected frame counts: frames=%d source=%d repair=%d", counts.frames, len(counts.source), len(counts.repair))
	}
}

// TestHardwareThroughput sends for one minute from A to B. Frame observations
// are authenticated locally and do not claim to measure radio-layer loss.
func TestHardwareThroughput(t *testing.T) {
	a, b := ports(t)
	duration := time.Minute
	if value := os.Getenv("EBYTE_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil || duration <= 0 {
			t.Fatalf("invalid EBYTE_DURATION: %q", value)
		}
	}
	payloadBytes := 512
	if value := os.Getenv("EBYTE_PAYLOAD_BYTES"); value != "" {
		var err error
		payloadBytes, err = strconv.Atoi(value)
		if err != nil || payloadBytes < 1 || payloadBytes > 4096 {
			t.Fatalf("invalid EBYTE_PAYLOAD_BYTES: %q (must be 1-4096)", value)
		}
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	sent, received := newFrameCounts(), newFrameCounts()
	mode := &serial.Mode{BaudRate: 9600, DataBits: 8, Parity: serial.NoParity, StopBits: serial.OneStopBit}
	portA, err := serial.Open(a, mode)
	if err != nil {
		t.Fatal(err)
	}
	portB, err := serial.Open(b, mode)
	if err != nil {
		portA.Close()
		t.Fatal(err)
	}
	cfg := fluid.DefaultConfig()
	cfg.Key = key
	cfg.MaxWireFrameBytes = 240
	cfg.AckTimeout = 10 * time.Second
	cfg.MaxRetries = 2
	if value := os.Getenv("EBYTE_INITIAL_REPAIRS"); value != "" {
		cfg.InitialRepairSymbols, err = strconv.Atoi(value)
		if err != nil || cfg.InitialRepairSymbols < 0 {
			portA.Close()
			portB.Close()
			t.Fatalf("invalid EBYTE_INITIAL_REPAIRS: %q", value)
		}
	}
	tx := &observedStream{ReadWriteCloser: portA, key: key, read: newFrameCounts(), written: sent,
		dropFirstSource: os.Getenv("EBYTE_DROP_FIRST_SOURCE") == "1"}
	left, err := fluid.New(tx, cfg)
	if err != nil {
		portA.Close()
		portB.Close()
		t.Fatal(err)
	}
	right, err := fluid.New(&observedStream{ReadWriteCloser: portB, key: key, read: received, written: newFrameCounts()}, cfg)
	if err != nil {
		left.Close()
		portB.Close()
		t.Fatal(err)
	}
	defer left.Close()
	defer right.Close()
	payload := make([]byte, payloadBytes)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration+45*time.Second)
	defer cancel()
	type receiveResult struct {
		messages int
		err      error
	}
	done := make(chan receiveResult, 1)
	go func() {
		count := 0
		for {
			msg, err := right.Receive(ctx)
			if err != nil {
				done <- receiveResult{count, err}
				return
			}
			if !bytes.Equal(msg, payload) {
				done <- receiveResult{count, errors.New("received payload differs")}
				return
			}
			count++
		}
	}()
	start := time.Now()
	acknowledged := 0
	var sendErr error
	for time.Since(start) < duration && ctx.Err() == nil {
		if err := left.Send(ctx, payload); err != nil {
			sendErr = err
			break
		}
		acknowledged++
	}
	elapsed := time.Since(start)
	cancel()
	result := <-done
	sent.mu.Lock()
	received.mu.Lock()
	t.Logf("elapsed=%s acknowledged_messages=%d received_messages=%d bytes=%d throughput=%.2f B/s (%.2f bit/s)",
		elapsed.Round(time.Millisecond), acknowledged, result.messages, result.messages*len(payload),
		float64(result.messages*len(payload))/elapsed.Seconds(), float64(result.messages*len(payload)*8)/elapsed.Seconds())
	t.Logf("RaptorQ data frames: transmitted=%d received=%d tx_wire_bytes=%d rx_wire_bytes=%d",
		sent.frames, received.frames, sent.wireBytes, received.wireBytes)
	t.Logf("RaptorQ distinct source symbols: transmitted=%d received=%d missing=%d",
		len(sent.source), len(received.source), countMissing(sent.source, received.source))
	t.Logf("RaptorQ distinct repair symbols: transmitted=%d received=%d missing=%d",
		len(sent.repair), len(received.repair), countMissing(sent.repair, received.repair))
	t.Logf("intentional source symbol drops=%d", tx.injectedDrops)
	received.mu.Unlock()
	sent.mu.Unlock()
	if sendErr != nil && !errors.Is(sendErr, context.DeadlineExceeded) {
		t.Errorf("send: %v", sendErr)
	}
	if result.err != nil && !errors.Is(result.err, context.Canceled) && !errors.Is(result.err, context.DeadlineExceeded) {
		t.Errorf("receive: %v", result.err)
	}
	if result.messages == 0 {
		t.Error("no messages received")
	}
	if acknowledged > result.messages {
		t.Error("more acknowledged than received messages")
	}
}
