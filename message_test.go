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
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/raptorq"
	"github.com/xtaci/lossyconn"
)

var testKey = bytes.Repeat([]byte{0x42}, 32)

// testConfig provides short retry timings and a shared test key.
func testConfig() Config {
	return Config{Key: testKey, MaxWireFrameBytes: 96, AckTimeout: 20 * time.Millisecond,
		MaxRetries: 10, ReceiveTimeout: time.Second, ReplayWindow: time.Minute}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxWireFrameBytes != 255 || cfg.MaxAppMessageBytes != 1<<20 ||
		cfg.AckTimeout != 3*time.Second || cfg.MaxRetries != 5 ||
		cfg.ReceiveTimeout != 2*time.Minute || cfg.ReplayWindow != 10*time.Minute {
		t.Fatalf("unexpected default config: %+v", cfg)
	}
	cfg.Key = testKey
	if _, err := cfg.normalized(); err != nil {
		t.Fatalf("default config is invalid: %v", err)
	}
}

func TestInitialRepairSymbols(t *testing.T) {
	for _, initial := range []int{0, 1} {
		t.Run(fmt.Sprintf("initial_%d", initial), func(t *testing.T) {
			cfg := testConfig()
			cfg.InitialRepairSymbols = initial
			cfg.AckTimeout = 400 * time.Millisecond
			a, b := net.Pipe()
			var repairs []uint32
			left, err := New(&filterStream{a, func(packet []byte) []byte {
				f := frameFromPacket(t, packet)
				if f.kind == frameData {
					if f.index == 0 {
						return nil
					}
					if f.index >= 4 {
						repairs = append(repairs, f.index)
					}
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
			defer left.Close()
			defer right.Close()
			payload := bytes.Repeat([]byte{7}, left.symbolSize()*4)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			start := time.Now()
			if err := left.Send(ctx, payload); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(start)
			got, err := right.Receive(ctx)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("receive: %v", err)
			}
			if len(repairs) == 0 || repairs[0] != 4 {
				t.Fatalf("unexpected repair symbols: %v", repairs)
			}
			if initial == 0 && elapsed < cfg.AckTimeout {
				t.Fatalf("repair sent before timeout: %s", elapsed)
			}
			if initial == 1 && elapsed >= cfg.AckTimeout {
				t.Fatalf("proactive repair did not avoid ACK timeout: %s", elapsed)
			}
		})
	}
}

func TestInvalidInitialRepairSymbols(t *testing.T) {
	for _, value := range []int{-1, maxSymbolIDLimit + 1} {
		cfg := testConfig()
		cfg.InitialRepairSymbols = value
		if _, err := cfg.normalized(); err == nil {
			t.Fatalf("expected error for InitialRepairSymbols=%d", value)
		}
	}
}

func TestInitialRepairContinuesWithNewRepairIDs(t *testing.T) {
	cfg := testConfig()
	cfg.InitialRepairSymbols = 1
	a, b := net.Pipe()
	var repairs []uint32
	left, err := New(&filterStream{a, func(packet []byte) []byte {
		f := frameFromPacket(t, packet)
		if f.kind == frameData {
			if f.index >= 4 {
				repairs = append(repairs, f.index)
			}
			if f.index == 0 || f.index == 4 {
				return nil
			}
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
	defer left.Close()
	defer right.Close()
	payload := bytes.Repeat([]byte{7}, left.symbolSize()*4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := left.Send(ctx, payload); err != nil {
		t.Fatal(err)
	}
	got, err := right.Receive(ctx)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("receive: %v", err)
	}
	if len(repairs) < 2 || repairs[0] != 4 || repairs[1] != 5 {
		t.Fatalf("repair IDs not advanced after lost proactive repair: %v", repairs)
	}
}

type filterStream struct {
	io.ReadWriteCloser
	filter func([]byte) []byte
}

type lossyPacketStream struct {
	conn    *lossyconn.LossyConn
	peer    net.Addr
	onRead  func([]byte)
	onWrite func([]byte)
}

type raptorqMessageStats struct {
	seen    map[raptorqSymbolID]struct{}
	symbols uint64
	bytes   uint64
}

type raptorqSymbolID struct {
	index uint32
}

type raptorqInputStats struct {
	mu       sync.Mutex
	messages map[[16]byte]*raptorqMessageStats
	sentIDs  map[uint32][16]byte
}

func (s *raptorqInputStats) record(packet []byte) {
	f, ok := parsedDataFrame(packet)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.messages == nil {
		s.messages = make(map[[16]byte]*raptorqMessageStats)
	}
	stats := s.messages[f.id]
	if stats == nil {
		stats = &raptorqMessageStats{seen: make(map[raptorqSymbolID]struct{})}
		s.messages[f.id] = stats
	}
	symbolID := raptorqSymbolID{index: f.index}
	if _, ok := stats.seen[symbolID]; ok {
		return
	}
	stats.seen[symbolID] = struct{}{}
	stats.symbols++
	stats.bytes += uint64(len(f.payload))
}

func (s *raptorqInputStats) recordSent(packet []byte) {
	f, ok := parsedDataFrame(packet)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sentIDs == nil {
		s.sentIDs = make(map[uint32][16]byte)
	}
	s.sentIDs[f.messageLen] = f.id
}

func parsedDataFrame(packet []byte) (frame, bool) {
	if len(packet) < 1 || packet[len(packet)-1] != 0 {
		return frame{}, false
	}
	f, err := parseFrame(packet[:len(packet)-1], testKey)
	if err != nil || f.kind != frameData || len(f.payload) != int(f.symbolSize) {
		return frame{}, false
	}
	return f, true
}

func (s *raptorqInputStats) messageID(messageLen uint32) [16]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sentIDs[messageLen]
}

func (s *raptorqInputStats) snapshot(id [16]byte) (symbols, bytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stats := s.messages[id]; stats != nil {
		return stats.symbols, stats.bytes
	}
	return 0, 0
}

func (s *lossyPacketStream) Read(p []byte) (int, error) {
	n, _, err := s.conn.ReadFrom(p)
	if n > 0 && s.onRead != nil {
		s.onRead(p[:n])
	}
	return n, err
}

func (s *lossyPacketStream) Write(p []byte) (int, error) {
	if s.onWrite != nil {
		s.onWrite(p)
	}
	return s.conn.WriteTo(p, s.peer)
}

func (s *lossyPacketStream) Close() error {
	return s.conn.Close()
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
func pair(t *testing.T, leftFilter, rightFilter func([]byte) []byte) (*Message, *Message) {
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

func lossyPair(t *testing.T, cfg lossyconn.Config, messageCfg Config, inputStats *raptorqInputStats) (*Message, *Message, *lossyconn.LossyConn, *lossyconn.LossyConn) {
	t.Helper()
	leftConn, err := lossyconn.NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rightCfg := cfg
	rightCfg.Seed++
	rightConn, err := lossyconn.NewWithConfig(rightCfg)
	if err != nil {
		_ = leftConn.Close()
		t.Fatal(err)
	}
	left, err := New(&lossyPacketStream{conn: leftConn, peer: rightConn.LocalAddr(), onWrite: inputStats.recordSent}, messageCfg)
	if err != nil {
		_ = leftConn.Close()
		_ = rightConn.Close()
		t.Fatal(err)
	}
	right, err := New(&lossyPacketStream{conn: rightConn, peer: leftConn.LocalAddr(), onRead: inputStats.record}, messageCfg)
	if err != nil {
		_ = left.Close()
		_ = rightConn.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	return left, right, leftConn, rightConn
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

// TestSendReceiveRepairSymbols checks repair symbols recover a corrupted source
// symbol and that acknowledgments are sent only after message completion.
func TestSendReceiveRepairSymbols(t *testing.T) {
	var mu sync.Mutex
	seen := map[uint32]int{}
	ackSeen := 0
	// Corrupt the first source symbol and drop the first completion ACK.
	left, right := pair(t, func(p []byte) []byte {
		f := frameFromPacket(t, p)
		if f.kind != frameData {
			return p
		}
		mu.Lock()
		defer mu.Unlock()
		seen[f.index]++
		if f.index == 0 && seen[f.index] == 1 {
			p[5] ^= 0x40
		}
		return p
	}, func(p []byte) []byte {
		f := frameFromPacket(t, p)
		if f.kind != frameAck {
			return p
		}
		mu.Lock()
		defer mu.Unlock()
		ackSeen++
		if ackSeen == 1 {
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
	sourceSymbols := (len(msg) + left.symbolSize() - 1) / left.symbolSize()
	for i := 0; i < sourceSymbols; i++ {
		if seen[uint32(i)] != 1 {
			t.Fatalf("source symbol %d was sent %d times, want exactly once: %v", i, seen[uint32(i)], seen)
		}
	}
	if len(seen) <= sourceSymbols {
		t.Fatalf("expected repair symbols after source loss: %v", seen)
	}
	if ackSeen < 2 {
		t.Fatalf("expected completion ACK retry after loss, got %d ACKs", ackSeen)
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

// TestAcknowledgmentHMACRequired checks that corrupted completion ACKs are rejected.
func TestAcknowledgmentHMACRequired(t *testing.T) {
	left, _ := pair(t, nil, func(p []byte) []byte {
		p[5] ^= 0x40
		return p
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := left.Send(ctx, []byte("data"))
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("message acknowledgment timeout")) {
		t.Fatalf("Send = %v, want message acknowledgment timeout", err)
	}
}

// TestCloseUnblocksReceive checks that a closed message returns its terminal error.
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
	message, err := New(a, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer message.Close()
	defer b.Close()
	go func() { _, _ = io.Copy(b, b) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = message.Send(ctx, []byte("echo"))
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("message acknowledgment timeout")) {
		t.Fatalf("Send = %v, want message acknowledgment timeout", err)
	}
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, err := message.Receive(short); !errors.Is(err, context.DeadlineExceeded) {
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

// TestLossyTransportMessageSizes exercises retransmission and reassembly across
// a matrix of bidirectional packet-loss rates and reports simulator statistics.
func TestLossyTransportMessageSizes(t *testing.T) {
	scenarios := []struct {
		name       string
		loss       float64
		corruption float64
		seed       int64
	}{
		{name: "loss_20pct", loss: 0.20, seed: 20261111},
		{name: "loss_40pct", loss: 0.40, seed: 20261112},
		{name: "loss_60pct", loss: 0.60, seed: 20261113},
		{name: "loss_20pct_corruption_10pct", loss: 0.20, corruption: 0.10, seed: 20261114},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			testLossyMessageSizes(t, scenario.loss, scenario.corruption, scenario.seed)
		})
	}
}

func testLossyMessageSizes(t *testing.T, lossRate, corruptionRate float64, seed int64) {
	t.Helper()
	lossCfg := lossyconn.Config{
		Delay:          2 * time.Millisecond,
		DelayDeviation: 0.5,
		RandomLossRate: lossRate,
		CorruptionRate: corruptionRate,
		MaxPacketSize:  255,
		Seed:           seed,
	}
	messageCfg := testConfig()
	// Each encoded frame is one lossyconn datagram, so the protocol's frame
	// limit must not exceed the simulated message's packet-size limit.
	messageCfg.MaxWireFrameBytes = lossCfg.MaxPacketSize
	messageCfg.AckTimeout = 20 * time.Millisecond
	messageCfg.MaxRetries = 100
	inputStats := &raptorqInputStats{}
	left, right, leftConn, rightConn := lossyPair(t, lossCfg, messageCfg, inputStats)
	symbolSize := left.symbolSize()
	if symbolSize <= 0 {
		t.Fatalf("invalid symbol size %d for max frame size %d", symbolSize, messageCfg.MaxWireFrameBytes)
	}
	maxFrame := marshalFrame(frame{
		kind:       frameData,
		messageLen: ^uint32(0),
		symbolSize: uint16(symbolSize),
		payload:    bytes.Repeat([]byte{0xff}, symbolSize),
	}, testKey)
	if len(maxFrame) > lossCfg.MaxPacketSize {
		t.Fatalf("maximum encoded frame size %d exceeds message packet limit %d", len(maxFrame), lossCfg.MaxPacketSize)
	}
	sizes := []int{1, 128, 4096, 16384}
	var totalRandomDrops, totalBurstDrops, totalCorrupted, totalDataCorrupted uint64
	var totalInputBytes, totalRestoredBytes, totalWireBytes uint64
	testStart := time.Now()

	for _, size := range sizes {
		msg := make([]byte, size)
		for i := range msg {
			msg[i] = byte(i*31 + size)
		}

		beforeLeft, beforeRight := leftConn.Stats(), rightConn.Stats()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		start := time.Now()
		sendErr := left.Send(ctx, msg)
		if sendErr != nil {
			cancel()
			t.Fatalf("Send(%d bytes): %v", size, sendErr)
		}
		got, receiveErr := right.Receive(ctx)
		cancel()
		elapsed := time.Since(start)
		if receiveErr != nil || !bytes.Equal(got, msg) {
			t.Fatalf("Receive(%d bytes): got %d bytes, err %v", size, len(got), receiveErr)
		}

		afterLeft, afterRight := leftConn.Stats(), rightConn.Stats()
		messageID := inputStats.messageID(uint32(size))
		inputSymbols, inputBytes := inputStats.snapshot(messageID)
		dataPackets := afterLeft.SentPackets - beforeLeft.SentPackets
		ackPackets := afterRight.SentPackets - beforeRight.SentPackets
		dataSymbols := uint64((size + left.symbolSize() - 1) / left.symbolSize())
		if inputSymbols < dataSymbols || inputBytes != inputSymbols*uint64(symbolSize) {
			t.Fatalf("message_bytes=%d: RaptorQ received %d unique symbols (%d bytes), need at least %d symbols",
				size, inputSymbols, inputBytes, dataSymbols)
		}
		wireBytes := afterLeft.SentBytes - beforeLeft.SentBytes
		randomDrops := afterLeft.DropRandom - beforeLeft.DropRandom +
			afterRight.DropRandom - beforeRight.DropRandom
		burstDrops := afterLeft.DropBurst - beforeLeft.DropBurst +
			afterRight.DropBurst - beforeRight.DropBurst
		corruptedPackets := afterLeft.CorruptedPackets - beforeLeft.CorruptedPackets +
			afterRight.CorruptedPackets - beforeRight.CorruptedPackets
		dataCorruptedPackets := afterLeft.CorruptedPackets - beforeLeft.CorruptedPackets
		oversizeDrops := afterLeft.DropOversize - beforeLeft.DropOversize +
			afterRight.DropOversize - beforeRight.DropOversize
		if oversizeDrops != 0 {
			t.Fatalf("message_bytes=%d produced %d frames over message packet limit %d",
				size, oversizeDrops, lossCfg.MaxPacketSize)
		}
		repairSymbols := dataPackets - dataSymbols
		totalRandomDrops += randomDrops
		totalBurstDrops += burstDrops
		totalCorrupted += corruptedPackets
		totalDataCorrupted += dataCorruptedPackets
		totalInputBytes += inputBytes
		totalRestoredBytes += uint64(len(got))
		totalWireBytes += wireBytes
		throughputMbps := float64(size*8) / elapsed.Seconds() / 1e6
		raptorqEfficiency := float64(len(got)) / float64(inputBytes) * 100
		wireEfficiency := float64(len(got)) / float64(wireBytes) * 100
		t.Logf("loss_rate=%.0f%% corruption_rate=%.0f%% message_bytes=%d symbol_bytes=%d source_symbols=%d raptorq_received_symbols=%d raptorq_input_bytes=%d restored_bytes=%d raptorq_efficiency=%.2f%% wire_bytes_with_repairs=%d wire_efficiency=%.2f%% max_frame_bytes=%d link_packet_limit=%d duration=%s throughput=%.3fMbps data_packets=%d repair_symbols=%d completion_ack_packets=%d random_drops=%d burst_drops=%d data_corrupted_packets=%d corrupted_packets=%d",
			lossRate*100, corruptionRate*100, size, symbolSize, dataSymbols, inputSymbols, inputBytes, len(got), raptorqEfficiency,
			wireBytes, wireEfficiency, len(maxFrame), lossCfg.MaxPacketSize,
			elapsed.Round(time.Millisecond), throughputMbps, dataPackets, repairSymbols, ackPackets,
			randomDrops, burstDrops, dataCorruptedPackets, corruptedPackets)
	}

	if totalRandomDrops+totalBurstDrops == 0 {
		t.Fatal("loss simulation produced no packet drops")
	}
	totalDuration := time.Since(testStart)
	if corruptionRate > 0 && totalDataCorrupted == 0 {
		t.Fatal("corruption simulation produced no corrupted data packets")
	}
	t.Logf("TOTAL loss_rate=%.0f%% corruption_rate=%.0f%% messages=%d raptorq_input_bytes=%d restored_bytes=%d raptorq_efficiency=%.2f%% data_wire_bytes_with_retries=%d wire_efficiency=%.2f%% duration=%s throughput=%.3fMbps random_drops=%d burst_drops=%d data_corrupted_packets=%d corrupted_packets=%d",
		lossRate*100, corruptionRate*100,
		len(sizes), totalInputBytes, totalRestoredBytes,
		float64(totalRestoredBytes)/float64(totalInputBytes)*100,
		totalWireBytes, float64(totalRestoredBytes)/float64(totalWireBytes)*100,
		totalDuration.Round(time.Millisecond),
		float64(totalRestoredBytes*8)/totalDuration.Seconds()/1e6,
		totalRandomDrops, totalBurstDrops, totalDataCorrupted, totalCorrupted)
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
	if _, err := cobsDecode(nil); err == nil {
		t.Fatal("empty COBS input accepted")
	}
	f := frame{kind: frameData, messageLen: 3, symbolSize: 3, payload: []byte("abc")}
	p := marshalFrame(f, testKey)
	if _, err := parseFrame(p[:len(p)-1], bytes.Repeat([]byte{1}, 32)); err == nil {
		t.Fatal("wrong HMAC key accepted")
	}
}

func TestFrameHMACMD5AndVersion(t *testing.T) {
	f := frame{kind: frameData, messageLen: 3, symbolSize: 3, payload: []byte("abc")}
	packet := marshalFrame(f, testKey)
	raw, err := cobsDecode(packet[:len(packet)-1])
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != headerSize+3+16 || raw[0] != 1 {
		t.Fatalf("unexpected frame size or version: %d, %d", len(raw), raw[0])
	}
	if got := hex.EncodeToString(raw[len(raw)-16:]); got != "30ea02a111ed2daa6a61db879c1a0282" {
		t.Fatalf("unexpected HMAC-MD5: %s", got)
	}
	decoded, err := parseFrame(packet[:len(packet)-1], testKey)
	if err != nil || !bytes.Equal(decoded.payload, f.payload) {
		t.Fatalf("parse valid frame: %v", err)
	}
	old := append([]byte(nil), raw[:len(raw)-16]...)
	old[0] = 1
	mac := hmac.New(sha256.New, testKey)
	mac.Write(old)
	old = append(old, mac.Sum(nil)...)
	if _, err := parseFrame(cobsEncode(old), testKey); err == nil {
		t.Fatal("legacy HMAC-SHA-256 frame accepted")
	}
	v2 := make([]byte, 40+len(f.payload))
	v2[0], v2[1] = 2, frameData
	copy(v2[40:], f.payload)
	v2MAC := hmac.New(md5.New, testKey)
	v2MAC.Write(v2)
	v2 = append(v2, v2MAC.Sum(nil)...)
	if _, err := parseFrame(cobsEncode(v2), testKey); err == nil || err.Error() != "unsupported frame" {
		t.Fatalf("authenticated version 2 frame should be rejected: %v", err)
	}
	for _, index := range []int{0, 1, 21, headerSize, len(raw) - 1} {
		bad := append([]byte(nil), raw...)
		bad[index] ^= 1
		if _, err := parseFrame(cobsEncode(bad), testKey); err == nil {
			t.Fatalf("tampering at byte %d accepted", index)
		}
	}
}

func TestFrameHMACProtectsSourceAndRepairSymbols(t *testing.T) {
	for _, index := range []uint32{0, 1} {
		f := frame{
			kind:       frameData,
			index:      index,
			messageLen: 3,
			symbolSize: 3,
			payload:    []byte("abc"),
		}
		packet := marshalFrame(f, testKey)
		raw, err := cobsDecode(packet[:len(packet)-1])
		if err != nil {
			t.Fatal(err)
		}
		raw[headerSize] ^= 0x01
		corrupted := cobsEncode(raw)
		if _, err := parseFrame(corrupted, testKey); err == nil {
			t.Fatalf("corrupted symbol payload accepted for symbol ID %d", index)
		}
	}
}

// TestConfigAndFrameLimit checks key validation and encoded frame size bounds.
func TestConfigAndFrameLimit(t *testing.T) {
	if _, err := (Config{}).normalized(); err == nil {
		t.Fatal("missing key accepted")
	}
	for _, max := range []int{76, 96, 254, 255, 256, 512, 65535} {
		cfg, err := (Config{Key: testKey, MaxWireFrameBytes: max}).normalized()
		if err != nil {
			t.Fatal(err)
		}
		l := &Message{cfg: cfg}
		f := frame{kind: frameData, payload: bytes.Repeat([]byte{1}, l.symbolSize())}
		if n := len(marshalFrame(f, testKey)); n > max {
			t.Fatalf("frame size %d exceeds %d", n, max)
		}
	}
	if _, err := (Config{Key: testKey, MaxWireFrameBytes: 75}).normalized(); err == nil {
		t.Fatal("wire frame limit below the minimum payload size accepted")
	}
}

func TestMessageSingleGenerationLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxWireFrameBytes = 76
	cfg.MaxAppMessageBytes = 4 << 20
	a, b := net.Pipe()
	message, err := New(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer message.Close()
	defer b.Close()
	limit := maxSourceSymbols * message.symbolSize()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := message.Send(ctx, make([]byte, limit)); !errors.Is(err, context.Canceled) {
		t.Fatalf("maximum generation rejected: %v", err)
	}
	if err := message.Send(ctx, make([]byte, limit+1)); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("oversized generation not rejected: %v", err)
	}
}

func TestMessageProtectsConfigKey(t *testing.T) {
	keyBytes := bytes.Repeat([]byte{0x73}, 32)
	cfg := testConfig()
	cfg.Key = keyBytes
	left, right := net.Pipe()
	message, err := New(left, cfg)
	if err != nil {
		_ = left.Close()
		_ = right.Close()
		t.Fatal(err)
	}
	defer right.Close()

	if message.cfg.Key != nil {
		t.Fatal("Message retained the key in ordinary Config memory")
	}
	if message.key == nil || !message.key.IsAlive() || !bytes.Equal(message.key.Bytes(), keyBytes) {
		t.Fatal("Message did not retain the key in protected memory")
	}
	if !bytes.Equal(keyBytes, bytes.Repeat([]byte{0x73}, 32)) {
		t.Fatal("New modified the caller's key slice")
	}
	if err := message.Close(); err != nil {
		t.Fatal(err)
	}
	if message.key.IsAlive() {
		t.Fatal("Message did not destroy the protected key on close")
	}
}

func TestRaptorQSymbolIDRange(t *testing.T) {
	if !validSymbolID(maxSourceSymbols - 1) {
		t.Fatal("last source symbol ID rejected")
	}
	if !validSymbolID(maxSourceSymbols) {
		t.Fatal("first repair symbol ID rejected")
	}
	if !validSymbolID(maxSymbolID) {
		t.Fatal("last 24-bit encoding symbol ID rejected")
	}
	if validSymbolID(maxSymbolID + 1) {
		t.Fatal("encoding symbol ID beyond 24-bit ESI range accepted")
	}

	enc, err := raptorq.NewRaptorQ(8).CreateEncoder([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	symbol := enc.GenSymbol(maxSymbolID)
	if len(symbol) != 8 {
		t.Fatalf("maximum repair symbol size = %d, want 8", len(symbol))
	}
	dec, err := raptorq.NewRaptorQ(8).CreateDecoder(4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dec.AddSymbol(maxSymbolID, symbol); err != nil {
		t.Fatalf("decoder rejected maximum repair symbol ID: %v", err)
	}
}
