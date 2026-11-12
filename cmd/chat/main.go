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
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/xtaci/fluid"
)

type chatLink interface {
	Send(context.Context, []byte) error
	Receive(context.Context) ([]byte, error)
	Close() error
}

func runChat(ctx context.Context, link chatLink, input io.Reader, output io.Writer, maxMessageBytes int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan error, 2)
	go func() {
		scanner := bufio.NewScanner(input)
		// Allow room for the line ending in addition to the message itself.
		scanner.Buffer(make([]byte, min(4096, maxMessageBytes+2)), maxMessageBytes+2)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			if err := link.Send(ctx, line); err != nil {
				results <- fmt.Errorf("send message (the peer may already have received it): %w", err)
				return
			}
		}
		if err := scanner.Err(); err != nil {
			results <- fmt.Errorf("read input: %w", err)
			return
		}
		results <- nil
	}()

	go func() {
		for {
			message, err := link.Receive(ctx)
			if err != nil {
				results <- fmt.Errorf("receive message: %w", err)
				return
			}
			if _, err := fmt.Fprintf(output, "peer: %s\n", message); err != nil {
				results <- fmt.Errorf("write received message: %w", err)
				return
			}
		}
	}()

	var err error
	select {
	case err = <-results:
	case <-ctx.Done():
	}
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		err = nil
	}
	cancel()
	if closeErr := link.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close link: %w", closeErr))
	}
	return err
}

func main() {
	port := flag.String("port", "", "serial port (for example COM3 or /dev/ttyUSB0)")
	baud := flag.Int("baud", 9600, "serial baud rate (8N1)")
	keyFile := flag.String("key", "", "file containing the shared key (at least 32 bytes)")
	cfg := fluid.DefaultConfig()
	flag.IntVar(&cfg.MaxWireFrameBytes, "frame", cfg.MaxWireFrameBytes, "maximum wire frame size in bytes")
	flag.DurationVar(&cfg.AckTimeout, "ack-timeout", cfg.AckTimeout, "acknowledgment timeout")
	flag.IntVar(&cfg.MaxRetries, "retries", cfg.MaxRetries, "maximum repair batches per chunk")
	flag.Parse()
	if *port == "" || *keyFile == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: chat -port PORT -key KEYFILE [options]")
		flag.PrintDefaults()
		os.Exit(2)
	}

	key, err := os.ReadFile(*keyFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read key: %v\n", err)
		os.Exit(1)
	}
	cfg.Key = key
	link, err := fluid.Open(*port, *baud, cfg)
	clear(key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open link: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	fmt.Fprintln(os.Stderr, "Connected. Enter a message per line; Ctrl-C or EOF exits.")
	if err := runChat(ctx, link, os.Stdin, os.Stdout, cfg.MaxAppMessageBytes); err != nil {
		fmt.Fprintf(os.Stderr, "chat: %v\n", err)
		os.Exit(1)
	}
}
