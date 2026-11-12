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
	"crypto/rand"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"testing"
	"time"

	"github.com/xtaci/fluid"
	"go.bug.st/serial"
)

func ports(t *testing.T) (string, string) {
	t.Helper()
	if os.Getenv("EBYTE_TEST") != "1" {
		t.Skip("set EBYTE_TEST=1 to run hardware tests")
	}
	a, b := os.Getenv("EBYTE_PORT_A"), os.Getenv("EBYTE_PORT_B")
	if a == "" || b == "" || a == b {
		t.Fatal("set EBYTE_PORT_A and EBYTE_PORT_B to distinct serial devices")
	}
	return a, b
}

func openPort(t *testing.T, name string) serial.Port {
	t.Helper()
	port, err := serial.Open(name, &serial.Mode{BaudRate: 9600, DataBits: 8, Parity: serial.NoParity, StopBits: serial.OneStopBit})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := port.Close(); err != nil {
			t.Errorf("close %s: %v", name, err)
		}
	})
	if err := port.SetReadTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	return port
}

// TestHardwareRegisters must only be run with both radios in configuration mode.
func TestHardwareRegisters(t *testing.T) {
	a, b := ports(t)
	if os.Getenv("EBYTE_CONFIG_MODE") != "1" {
		t.Skip("set EBYTE_CONFIG_MODE=1 only when both radios are in configuration mode")
	}
	var values [2][]byte
	for i, name := range []string{a, b} {
		p := openPort(t, name)
		if err := p.ResetInputBuffer(); err != nil {
			t.Fatal(err)
		}
		if n, err := p.Write([]byte{0xc1, 0x00, 0x07}); err != nil || n != 3 {
			t.Fatalf("%s read command write: %d bytes, %v", name, n, err)
		}
		buf := make([]byte, 10)
		n := 0
		for deadline := time.Now().Add(4 * time.Second); n < len(buf) && time.Now().Before(deadline); {
			got, err := p.Read(buf[n:])
			if err != nil {
				t.Fatalf("%s read: %v", name, err)
			}
			n += got
		}
		if n != len(buf) {
			t.Fatalf("%s incomplete response (%d bytes)", name, n)
		}
		if !bytes.Equal(buf[:3], []byte{0xc1, 0, 7}) {
			t.Fatalf("%s unexpected response % X", name, buf)
		}
		values[i] = buf[3:]
		t.Logf("%s registers 00-06: % X", name, values[i])
	}
	if !bytes.Equal(values[0], values[1]) {
		t.Error("readable registers differ; write-only radio keys cannot be compared")
	}
}

// TestHardwareRaw checks transparent serial delivery before using Link.
func TestHardwareRaw(t *testing.T) {
	a, b := ports(t)
	left, right := openPort(t, a), openPort(t, b)
	for _, step := range []struct {
		name string
		from serial.Port
		to   serial.Port
		text []byte
	}{
		{"A-to-B", left, right, []byte("fluid-raw-test-A-to-B\n")},
		{"B-to-A", right, left, []byte("fluid-raw-test-B-to-A\n")},
	} {
		t.Run(step.name, func(t *testing.T) {
			if err := step.to.ResetInputBuffer(); err != nil {
				t.Fatal(err)
			}
			n, err := step.from.Write(step.text)
			if err != nil || n != len(step.text) {
				t.Fatalf("write %d/%d: %v", n, len(step.text), err)
			}
			var received []byte
			for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
				buf := make([]byte, 256)
				n, err := step.to.Read(buf)
				if err != nil {
					t.Fatal(err)
				}
				received = append(received, buf[:n]...)
				if bytes.Contains(received, step.text) {
					t.Logf("received %d bytes", len(received))
					return
				}
			}
			t.Fatalf("test marker not received; got %d bytes", len(received))
		})
	}
}

func openLinks(t *testing.T) (*fluid.Link, *fluid.Link) {
	t.Helper()
	a, b := ports(t)
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	cfg := fluid.DefaultConfig()
	cfg.Key = key
	cfg.MaxWireFrameBytes = 240
	cfg.AckTimeout = 10 * time.Second
	cfg.MaxRetries = 2
	open := func(name string) *fluid.Link {
		link, err := fluid.Open(name, 9600, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := link.Close(); err != nil {
				t.Errorf("close %s: %v", name, err)
			}
		})
		return link
	}
	return open(a), open(b)
}

func TestHardwareLink(t *testing.T) {
	left, right := openLinks(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	for _, step := range []struct {
		name string
		from *fluid.Link
		to   *fluid.Link
		text string
	}{
		{"A-to-B", left, right, "fluid test A to B"},
		{"B-to-A", right, left, "fluid test B to A"},
	} {
		if err := step.from.Send(ctx, []byte(step.text)); err != nil {
			t.Fatalf("%s send: %v", step.name, err)
		}
		got, err := step.to.Receive(ctx)
		if err != nil || string(got) != step.text {
			t.Fatalf("%s receive: %q, %v", step.name, got, err)
		}
		t.Logf("%s: acknowledged and received %q", step.name, got)
	}
}

func TestHardwareImage(t *testing.T) {
	left, right := openLinks(t)
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var source bytes.Buffer
	if err := png.Encode(&source, img); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, step := range []struct {
		name string
		from *fluid.Link
		to   *fluid.Link
	}{
		{"A-to-B", left, right},
		{"B-to-A", right, left},
	} {
		var output bytes.Buffer
		result := make(chan error, 1)
		go func() {
			n, err := step.to.ReceiveStream(ctx, &output, uint64(source.Len()))
			if err == nil && n != uint64(source.Len()) {
				err = fmt.Errorf("received %d bytes, expected %d", n, source.Len())
			}
			result <- err
		}()
		n, err := step.from.SendStream(ctx, bytes.NewReader(source.Bytes()), 64)
		if err != nil {
			t.Fatalf("%s send: %v", step.name, err)
		}
		if err := <-result; err != nil {
			t.Fatalf("%s receive: %v", step.name, err)
		}
		if !bytes.Equal(source.Bytes(), output.Bytes()) {
			t.Fatalf("%s: PNG bytes differ", step.name)
		}
		if _, err := png.Decode(bytes.NewReader(output.Bytes())); err != nil {
			t.Fatalf("%s: decode PNG: %v", step.name, err)
		}
		t.Logf("%s: verified %d-byte PNG", step.name, n)
	}
}
