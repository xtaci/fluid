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

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xtaci/fluid"
)

type messageWriter chan string

func (w messageWriter) Write(p []byte) (int, error) {
	w <- string(p)
	return len(p), nil
}

func waitMessage(t *testing.T, messages <-chan string, want string) {
	t.Helper()
	select {
	case got := <-messages:
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func waitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for chat to stop")
		return nil
	}
}

func TestChatBetweenLinks(t *testing.T) {
	left, right := net.Pipe()
	cfg := fluid.DefaultConfig()
	cfg.Key = bytes.Repeat([]byte{0x42}, 32)
	a, err := fluid.New(left, cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := fluid.New(right, cfg)
	if err != nil {
		a.Close()
		t.Fatal(err)
	}

	inputA, writerA := io.Pipe()
	inputB, writerB := io.Pipe()
	t.Cleanup(func() {
		writerA.Close()
		writerB.Close()
		inputA.Close()
		inputB.Close()
		a.Close()
		b.Close()
	})
	outputA, outputB := make(messageWriter, 1), make(messageWriter, 1)
	resultA, resultB := make(chan error, 1), make(chan error, 1)
	go func() { resultA <- runChat(context.Background(), a, inputA, outputA, cfg.MaxAppMessageBytes) }()
	go func() { resultB <- runChat(context.Background(), b, inputB, outputB, cfg.MaxAppMessageBytes) }()

	if _, err := io.WriteString(writerA, "\nhello\n"); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, outputB, "peer: hello\n")
	if _, err := io.WriteString(writerA, "\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(writerB, "你好\n"); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, outputA, "peer: 你好\n")
	if _, err := io.WriteString(writerB, "\n"); err != nil {
		t.Fatal(err)
	}

	if err := writerA.Close(); err != nil {
		t.Fatal(err)
	}
	if err := waitResult(t, resultA); err != nil {
		t.Fatalf("EOF: %v", err)
	}
	if err := waitResult(t, resultB); err == nil {
		t.Fatal("peer disconnect should be reported")
	}
}

type failingLink struct {
	sendErr  error
	closeErr error
}

func (l *failingLink) Send(context.Context, []byte) error { return l.sendErr }
func (l *failingLink) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (l *failingLink) Close() error { return l.closeErr }

func TestSendFailureReported(t *testing.T) {
	sendErr := errors.New("send failed")
	closeErr := errors.New("close failed")
	err := runChat(context.Background(), &failingLink{sendErr: sendErr, closeErr: closeErr},
		strings.NewReader("message\n"), io.Discard, 1024)
	if !errors.Is(err, sendErr) || !errors.Is(err, closeErr) {
		t.Fatalf("expected send and close errors, got %v", err)
	}
	if !strings.Contains(err.Error(), "peer may already have received") {
		t.Fatalf("missing delivery warning: %v", err)
	}
}

func TestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	result := make(chan error, 1)
	go func() {
		result <- runChat(ctx, &failingLink{}, input, io.Discard, 1024)
	}()
	cancel()
	if err := waitResult(t, result); err != nil {
		t.Fatalf("cancellation: %v", err)
	}
}

type recordingLink struct {
	failingLink
	messages []string
}

func (l *recordingLink) Send(_ context.Context, data []byte) error {
	l.messages = append(l.messages, string(data))
	return nil
}

func TestInputLinesAndEOF(t *testing.T) {
	link := &recordingLink{}
	err := runChat(context.Background(), link, strings.NewReader("\nfirst\r\nlast"), io.Discard, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(link.messages) != 2 || link.messages[0] != "first" || link.messages[1] != "last" {
		t.Fatalf("unexpected messages: %q", link.messages)
	}
}

func TestOversizedInputReported(t *testing.T) {
	err := runChat(context.Background(), &recordingLink{}, strings.NewReader(strings.Repeat("x", 19)),
		io.Discard, 16)
	if err == nil || !strings.Contains(err.Error(), "read input") {
		t.Fatalf("expected input error, got %v", err)
	}
}
