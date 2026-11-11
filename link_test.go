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

var testKey = bytes.Repeat([]byte{0x42}, 32)

// testConfig provides short retry timings and a shared test key.
func testConfig() Config {
	return Config{Key: testKey, MaxFrameSize: 96, AckTimeout: 20 * time.Millisecond,
		MaxRetries: 10, ReceiveTimeout: time.Second, ReplayWindow: time.Minute}
}

type filterStream struct {
	io.ReadWriteCloser
	filter func([]byte) []byte
}

// Write applies the test filter to simulate frame corruption or loss.
func (s *filterStream) Write(p []byte) (int, error) {
	if s.filter != nil {
		data := s.filter(append([]byte(nil), p...))
		if data == nil {
			return len(p), nil
		}
		_, err := s.ReadWriteCloser.Write(data)
		return len(p), err
	}
	return s.ReadWriteCloser.Write(p)
}

// pair creates two linked endpoints with optional directional frame filters.
func pair(t *testing.T, leftFilter, rightFilter func([]byte) []byte) (*Link, *Link) {
	t.Helper()
	a, b := net.Pipe()
	left, err := New(&filterStream{a, leftFilter}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	right, err := New(&filterStream{b, rightFilter}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	return left, right
}

// frameFromPacket verifies and decodes a framed packet in a test filter.
func frameFromPacket(t *testing.T, p []byte) frame {
	t.Helper()
	if len(p) == 0 || p[len(p)-1] != 0 {
		t.Fatal("missing packet delimiter")
	}
	f, err := parseFrame(p[:len(p)-1], testKey)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestSendReceiveRetriesAndAuthentication checks retries after corruption and lost ACKs.
func TestSendReceiveRetriesAndAuthentication(t *testing.T) {
	var mu sync.Mutex
	seen := map[uint32]int{}
	ackSeen := map[uint32]int{}
	// Corrupt the first data frame and drop the second symbol's first ACK.
	left, right := pair(t, func(p []byte) []byte {
		f := frameFromPacket(t, p)
		mu.Lock()
		defer mu.Unlock()
		seen[f.index]++
		if f.index == 0 && seen[f.index] == 1 {
			p[5] ^= 0x40
		}
		return p
	}, func(p []byte) []byte {
		f := frameFromPacket(t, p)
		mu.Lock()
		defer mu.Unlock()
		ackSeen[f.index]++
		if f.index == 1 && ackSeen[f.index] == 1 {
			return nil
		}
		return p
	})
	msg := bytes.Repeat([]byte("hello\x00LoRa"), 23)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := left.Send(ctx, msg); err != nil {
		t.Fatal(err)
	}
	got, err := right.Receive(ctx)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("Receive = %q, %v", got, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if seen[0] < 2 || seen[1] < 2 || len(seen) < 3 {
		t.Fatalf("expected per-symbol retries and multiple symbols: %v", seen)
	}
	for i := range seen {
		if ackSeen[i] == 0 {
			t.Fatalf("symbol %d was not acknowledged", i)
		}
	}
}

// TestFinalAckRetryDoesNotDeliverTwice checks deduplication after a lost final ACK.
func TestFinalAckRetryDoesNotDeliverTwice(t *testing.T) {
	var dropped bool
	left, right := pair(t, nil, func(p []byte) []byte {
		f := frameFromPacket(t, p)
		if f.index == 0 && !dropped {
			dropped = true
			return nil
		}
		return p
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := left.Send(ctx, []byte("one symbol")); err != nil {
		t.Fatal(err)
	}
	msg, err := right.Receive(ctx)
	if err != nil || string(msg) != "one symbol" {
		t.Fatalf("Receive = %q, %v", msg, err)
	}
	short, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	if _, err := right.Receive(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("duplicate message: %v", err)
	}
}

// TestFullReceiveQueueWithholdsFinalAck checks backpressure on message delivery.
func TestFullReceiveQueueWithholdsFinalAck(t *testing.T) {
	left, right := pair(t, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := left.Send(ctx, []byte("first")); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- left.Send(ctx, []byte("second")) }()
	select {
	case err := <-result:
		t.Fatalf("second send finished before queue drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if msg, err := right.Receive(ctx); err != nil || string(msg) != "first" {
		t.Fatalf("first Receive = %q, %v", msg, err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if msg, err := right.Receive(ctx); err != nil || string(msg) != "second" {
		t.Fatalf("second Receive = %q, %v", msg, err)
	}
}

// TestTimeoutAndCancellation checks cancellation and empty-message rejection.
func TestTimeoutAndCancellation(t *testing.T) {
	left, _ := pair(t, func([]byte) []byte { return nil }, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	if err := left.Send(ctx, []byte("data")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send = %v", err)
	}
	if err := left.Send(context.Background(), nil); err == nil {
		t.Fatal("empty message accepted")
	}
}

// TestAcknowledgmentHMACRequired checks that corrupted ACKs cannot complete a send.
func TestAcknowledgmentHMACRequired(t *testing.T) {
	left, _ := pair(t, nil, func(p []byte) []byte {
		p[5] ^= 0x40
		return p
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := left.Send(ctx, []byte("data"))
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("acknowledgment timeout")) {
		t.Fatalf("Send = %v, want acknowledgment timeout", err)
	}
}

// TestCloseUnblocksReceive checks that a closed link returns its terminal error.
func TestCloseUnblocksReceive(t *testing.T) {
	left, _ := pair(t, nil, nil)
	if err := left.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := left.Receive(context.Background()); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Receive = %v", err)
	}
}

// TestQueuedMessageSurvivesDisconnect checks delivery of queued data after close.
func TestQueuedMessageSurvivesDisconnect(t *testing.T) {
	left, right := pair(t, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := left.Send(ctx, []byte("delivered")); err != nil {
		t.Fatal(err)
	}
	if err := right.Close(); err != nil {
		t.Fatal(err)
	}
	msg, err := right.Receive(ctx)
	if err != nil || string(msg) != "delivered" {
		t.Fatalf("Receive after close = %q, %v", msg, err)
	}
}

// TestEchoCannotAcknowledgeOwnMessage checks that serial echoes are ignored.
func TestEchoCannotAcknowledgeOwnMessage(t *testing.T) {
	a, b := net.Pipe()
	link, err := New(a, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	defer b.Close()
	go func() { _, _ = io.Copy(b, b) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = link.Send(ctx, []byte("echo"))
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("acknowledgment timeout")) {
		t.Fatalf("Send = %v, want acknowledgment timeout", err)
	}
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, err := link.Receive(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("received own message: %v", err)
	}
}

// TestLargeMessage checks reassembly across many source symbols.
func TestLargeMessage(t *testing.T) {
	left, right := pair(t, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg := bytes.Repeat([]byte{0, 1, 2, 0xff}, 2048)
	if err := left.Send(ctx, msg); err != nil {
		t.Fatal(err)
	}
	got, err := right.Receive(ctx)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("large message: length %d, error %v", len(got), err)
	}
}

// TestFrameEncoding checks COBS boundary cases and HMAC key verification.
func TestFrameEncoding(t *testing.T) {
	for _, n := range []int{0, 1, 253, 254, 255, 512} {
		for _, data := range [][]byte{make([]byte, n), bytes.Repeat([]byte{1}, n)} {
			encoded := cobsEncode(data)
			if bytes.Contains(encoded, []byte{0}) {
				t.Fatal("encoded frame contains delimiter")
			}
			got, err := cobsDecode(encoded)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("COBS round trip length %d: %v", n, err)
			}
		}
	}
	if _, err := cobsDecode([]byte{3, 1}); err == nil {
		t.Fatal("invalid COBS accepted")
	}
	f := frame{kind: frameData, messageLen: 3, symbolSize: 3, payload: []byte("abc")}
	p := marshalFrame(f, testKey)
	if _, err := parseFrame(p[:len(p)-1], bytes.Repeat([]byte{1}, 32)); err == nil {
		t.Fatal("wrong HMAC key accepted")
	}
}

// TestConfigAndFrameLimit checks key validation and encoded frame size bounds.
func TestConfigAndFrameLimit(t *testing.T) {
	if _, err := (Config{}).normalized(); err == nil {
		t.Fatal("missing key accepted")
	}
	for _, max := range []int{64, 96, 254, 255, 256, 512, 65535} {
		cfg, err := (Config{Key: testKey, MaxFrameSize: max}).normalized()
		if err != nil {
			t.Fatal(err)
		}
		l := &Link{cfg: cfg}
		f := frame{kind: frameData, payload: bytes.Repeat([]byte{1}, l.symbolSize())}
		if n := len(marshalFrame(f, testKey)); n > max {
			t.Fatalf("frame size %d exceeds %d", n, max)
		}
	}
}
