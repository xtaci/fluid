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
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func sessionPair(t *testing.T, cfg Config, dc SessionConfig) (*Session, *Session) {
	t.Helper()
	a, b := net.Pipe()
	left, err := New(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	right, err := New(b, cfg)
	if err != nil {
		left.Close()
		t.Fatal(err)
	}
	dc.Primary = false
	secondary, err := NewSession(right, dc)
	if err != nil {
		left.Close()
		right.Close()
		t.Fatal(err)
	}
	dc.Primary = true
	primary, err := NewSession(left, dc)
	if err != nil {
		secondary.Close()
		left.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { primary.Close(); secondary.Close() })
	return primary, secondary
}

func TestSessionBidirectional(t *testing.T) {
	cfg := testConfig()
	dc := SessionConfig{PollInterval: time.Millisecond, ExchangeTimeout: time.Second, BufferBytes: 32, BlockBytes: 16}
	a, b := sessionPair(t, cfg, dc)
	dataA := bytes.Repeat([]byte{0, 1, 2, 255}, 200)
	dataB := bytes.Repeat([]byte{3, 0, 9}, 300)
	results := make(chan error, 4)
	go func() { _, err := a.Write(dataA); results <- err }()
	go func() { _, err := b.Write(dataB); results <- err }()
	go func() {
		got := make([]byte, len(dataB))
		_, err := io.ReadFull(a, got)
		if err == nil && !bytes.Equal(got, dataB) {
			err = errors.New("primary received wrong data")
		}
		results <- err
	}()
	go func() {
		got := make([]byte, len(dataA))
		_, err := io.ReadFull(b, got)
		if err == nil && !bytes.Equal(got, dataA) {
			err = errors.New("secondary received wrong data")
		}
		results <- err
	}()
	for i := 0; i < 4; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("bidirectional transfer stalled")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSessionSplitsWritesIntoMessages(t *testing.T) {
	cfg := testConfig()
	a, b := net.Pipe()
	var mu sync.Mutex
	seen := make(map[[16]byte]uint32)
	left, err := New(&filterStream{a, func(packet []byte) []byte {
		f := frameFromPacket(t, packet)
		if f.kind == frameData {
			mu.Lock()
			seen[f.id] = f.messageLen
			mu.Unlock()
		}
		return packet
	}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	right, err := New(b, cfg)
	if err != nil {
		left.Close()
		t.Fatal(err)
	}
	dc := SessionConfig{PollInterval: time.Millisecond, ExchangeTimeout: time.Second, BufferBytes: 128, BlockBytes: 16}
	secondary, err := NewSession(right, dc)
	if err != nil {
		left.Close()
		right.Close()
		t.Fatal(err)
	}
	defer secondary.Close()
	dc.Primary = true
	primary, err := NewSession(left, dc)
	if err != nil {
		left.Close()
		t.Fatal(err)
	}
	defer primary.Close()
	payload := bytes.Repeat([]byte{1, 0, 255}, 34)
	if _, err := primary.Write(payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(secondary, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("data differs after packetization")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 7 {
		t.Fatalf("expected at least seven bounded messages, got %d", len(seen))
	}
	for _, size := range seen {
		if size > sessionHeaderSize+uint32(dc.BlockBytes) {
			t.Fatalf("message exceeds session block limit: %d", size)
		}
	}
}

func TestSessionBackpressureAndClose(t *testing.T) {
	a, _ := sessionPair(t, testConfig(), SessionConfig{
		PollInterval: time.Millisecond, ExchangeTimeout: time.Second, BufferBytes: 8, BlockBytes: 8,
	})
	result := make(chan error, 1)
	go func() { _, err := a.Write(bytes.Repeat([]byte{1}, 100)); result <- err }()
	select {
	case err := <-result:
		t.Fatalf("write unexpectedly finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("expected closed pipe, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock write")
	}
}

func TestSessionInvalidConfig(t *testing.T) {
	a, b := net.Pipe()
	cfg := testConfig()
	cfg.MaxAppMessageBytes = 16 << 20
	message, err := New(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer message.Close()
	defer b.Close()
	if _, err := NewSession(message, SessionConfig{BufferBytes: 4, BlockBytes: 5}); err == nil {
		t.Fatal("expected invalid buffer configuration")
	}
	limit := maxSourceSymbols*message.symbolSize() - sessionHeaderSize
	if _, err := NewSession(message, SessionConfig{BufferBytes: limit + 1, BlockBytes: limit + 1}); err == nil {
		t.Fatal("expected RaptorQ generation limit rejection")
	}
	session, err := NewSession(message, SessionConfig{BufferBytes: limit, BlockBytes: limit})
	if err != nil {
		t.Fatalf("generation limit should be accepted: %v", err)
	}
	session.Close()
}

func TestSessionLargeWrite(t *testing.T) {
	cfg := testConfig()
	cfg.ReplayWindow = 100 * time.Millisecond
	a, b := sessionPair(t, cfg, SessionConfig{
		PollInterval: time.Millisecond, ExchangeTimeout: 2 * time.Second, BufferBytes: 4096, BlockBytes: 1024,
	})
	input := bytes.Repeat([]byte{0, 1, 2, 255, 17}, 30<<10)
	result := make(chan error, 2)
	go func() {
		got := make([]byte, len(input))
		_, err := io.ReadFull(b, got)
		if err == nil && !bytes.Equal(got, input) {
			err = errors.New("large session write differs")
		}
		result <- err
	}()
	go func() {
		_, err := a.Write(input)
		result <- err
	}()
	deadline := time.After(10 * time.Second)
	for range 2 {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatal("large session write stalled")
		}
	}
}

func TestSessionReplayWindowPacing(t *testing.T) {
	cfg := testConfig()
	cfg.ReplayWindow = 150 * time.Millisecond
	a, b := sessionPair(t, cfg, SessionConfig{
		PollInterval: time.Millisecond, ExchangeTimeout: time.Second, BufferBytes: 256, BlockBytes: 1,
	})
	input := bytes.Repeat([]byte{7}, 140)
	result := make(chan error, 1)
	go func() {
		got := make([]byte, len(input))
		_, err := io.ReadFull(b, got)
		if err == nil && !bytes.Equal(got, input) {
			err = errors.New("incorrect received bytes")
		}
		result <- err
	}()
	if _, err := a.Write(input); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("transfer stalled after replay window filled")
	}
}

func TestSessionRecoversSourceLoss(t *testing.T) {
	cfg := testConfig()
	cfg.InitialRepairSymbols = 1
	a, b := net.Pipe()
	left, err := New(&filterStream{a, func(packet []byte) []byte {
		f := frameFromPacket(t, packet)
		if f.kind == frameData && f.index == 0 {
			return nil
		}
		return packet
	}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	right, err := New(b, cfg)
	if err != nil {
		left.Close()
		t.Fatal(err)
	}
	dc := SessionConfig{PollInterval: time.Millisecond, ExchangeTimeout: time.Second, BufferBytes: 256, BlockBytes: 128}
	secondary, err := NewSession(right, dc)
	if err != nil {
		t.Fatal(err)
	}
	defer secondary.Close()
	dc.Primary = true
	primary, err := NewSession(left, dc)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	data := bytes.Repeat([]byte{1, 0, 3}, 50)
	if _, err := primary.Write(data); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		got := make([]byte, len(data))
		_, err := io.ReadFull(secondary, got)
		if err == nil && !bytes.Equal(data, got) {
			err = errors.New("wrong recovered data")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loss recovery stalled")
	}
}

func TestSessionCloseUnblocksRead(t *testing.T) {
	a, _ := sessionPair(t, testConfig(), SessionConfig{
		PollInterval: time.Millisecond, ExchangeTimeout: time.Second, BufferBytes: 8, BlockBytes: 8,
	})
	result := make(chan error, 1)
	go func() { _, err := a.Read(make([]byte, 1)); result <- err }()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-result:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("read returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock read")
	}
}

func TestSessionDetectsPeerFailure(t *testing.T) {
	cfg := testConfig()
	cfg.ReplayWindow = 100 * time.Millisecond
	a, b := sessionPair(t, cfg, SessionConfig{
		PollInterval: 5 * time.Millisecond, ExchangeTimeout: 250 * time.Millisecond, BufferBytes: 32, BlockBytes: 16,
	})
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := a.Read(make([]byte, 1)); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected peer failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer failure not reported")
	}
}
