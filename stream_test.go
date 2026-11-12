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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStreamRoundTrip(t *testing.T) {
	var pngData bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 12, 12))
	img.Set(3, 5, color.RGBA{R: 255, A: 255})
	if err := png.Encode(&pngData, img); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"image", pngData.Bytes()},
		{"large-binary", bytes.Repeat([]byte{0, 1, 2, 255, 13, 10, 128}, (2<<20)/7+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			cfg := testConfig()
			cfg.MaxWireFrameBytes = 1024
			cfg.AckTimeout = time.Second
			left, err := New(a, cfg)
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
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			dir := t.TempDir()
			sourcePath := filepath.Join(dir, "source.bin")
			if err := os.WriteFile(sourcePath, tc.data, 0600); err != nil {
				t.Fatal(err)
			}
			src, err := os.Open(sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			dst, err := os.Create(filepath.Join(dir, "received.bin"))
			if err != nil {
				t.Fatal(err)
			}
			defer dst.Close()
			receive := make(chan error, 1)
			go func() {
				n, err := right.ReceiveStream(ctx, dst, uint64(len(tc.data)))
				if err == nil && n != uint64(len(tc.data)) {
					err = errors.New("wrong receive byte count")
				}
				receive <- err
			}()
			n, err := left.SendStream(ctx, src, 64<<10)
			if err != nil || n != uint64(len(tc.data)) {
				t.Fatalf("SendStream = %d, %v", n, err)
			}
			if err := <-receive; err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(dst.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.data) {
				t.Fatal("received bytes differ")
			}
		})
	}
}

func TestStreamReplayWindowPacing(t *testing.T) {
	a, b := net.Pipe()
	cfg := testConfig()
	cfg.ReplayWindow = 100 * time.Millisecond
	left, err := New(a, cfg)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data := bytes.Repeat([]byte{42}, 130)
	result := make(chan error, 1)
	go func() {
		_, err := right.ReceiveStream(ctx, io.Discard, uint64(len(data)))
		result <- err
	}()
	if _, err := left.SendStream(ctx, bytes.NewReader(data), 1); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestStreamRejectsInvalidDigestAndSize(t *testing.T) {
	for _, tc := range []struct {
		name string
		max  uint64
	}{
		{"digest", 10},
		{"size", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left, right := pair(t, nil, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := right.ReceiveStream(ctx, io.Discard, tc.max)
				result <- err
			}()
			id := [16]byte{1}
			if err := left.Send(ctx, append(streamHeader(streamData, id, 0), []byte("abc")...)); err != nil {
				t.Fatal(err)
			}
			if tc.name == "digest" {
				end := binary.BigEndian.AppendUint64(streamHeader(streamEnd, id, 1), 3)
				end = append(end, make([]byte, sha256.Size)...)
				if err := left.Send(ctx, end); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-result; err == nil {
				t.Fatal("expected invalid stream to be rejected")
			}
		})
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestStreamWriterFailure(t *testing.T) {
	left, right := pair(t, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := right.ReceiveStream(ctx, shortWriter{}, 100)
		result <- err
	}()
	if err := left.Send(ctx, append(streamHeader(streamData, [16]byte{1}, 0), []byte("abc")...)); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected short write, got %v", err)
	}
}

func TestStreamSenderDoesNotSucceedOnReceiverFailure(t *testing.T) {
	left, right := pair(t, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := right.ReceiveStream(ctx, shortWriter{}, 100)
		result <- err
	}()
	if _, err := left.SendStream(ctx, strings.NewReader("abc"), 64); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("sender should time out without confirmation, got %v", err)
	}
	if err := <-result; !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected destination error, got %v", err)
	}
}

func TestStreamRejectsUnexpectedSequence(t *testing.T) {
	left, right := pair(t, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := right.ReceiveStream(ctx, io.Discard, 10)
		result <- err
	}()
	if err := left.Send(ctx, append(streamHeader(streamData, [16]byte{1}, 1), 'x')); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil || !strings.Contains(err.Error(), "sequence") {
		t.Fatalf("expected sequence error, got %v", err)
	}
}

func TestStreamInvalidArguments(t *testing.T) {
	left, right := pair(t, nil, nil)
	if _, err := left.SendStream(context.Background(), strings.NewReader("a"), 0); err == nil {
		t.Fatal("expected invalid block size")
	}
	if _, err := right.ReceiveStream(context.Background(), nil, 1); err == nil {
		t.Fatal("expected nil destination error")
	}
}
