# fluid

A Go link layer for transparent LoRa serial modules. It uses RaptorQ source and repair symbols, authenticates each symbol and message-completion acknowledgment with HMAC-SHA-256, and reassembles complete messages for the application.

## Usage

Both endpoints must be provisioned securely with the same random key of at least 32 bytes. Do not hard-code the key in the program or repository.

```go
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/xtaci/fluid"
)

func main() {
	// Both endpoints must use the same file containing at least 32 random bytes.
	key, err := os.ReadFile("link.key")
	if err != nil {
		log.Fatal(err)
	}
	link, err := fluid.Open("/dev/ttyUSB0", 9600, fluid.Config{Key: key})
	if err != nil {
		log.Fatal(err)
	}
	defer link.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := link.Send(ctx, []byte("hello LoRa")); err != nil {
		log.Fatal(err)
	}
}
```

The receiving endpoint opens its own serial port in the same way and calls `message, err := link.Receive(ctx)`. Alternatively, use `New(stream, config)` with any transparent bidirectional stream implementing `io.ReadWriteCloser`. The link owns the stream, and its `Close` method must unblock pending reads and writes.

The context passed to `Send` can cancel waiting for the send lock and acknowledgments, but cannot interrupt a blocked underlying `Write`; close the link to interrupt it. Even when `Send` returns an error, the peer may already have received the message. Messages already queued can still be read with `Receive` after the link closes.

The serial port uses 8N1. Configure the module's frequency, radio parameters, and transparent mode beforehand. This library does not implement vendor-specific AT commands, addressing, transmit/receive switching, or hardware flow control. Both endpoints must use the same key and compatible frame size settings. `MaxFrameSize` includes the complete serial frame and must not exceed the module's single radio payload limit; the module must not insert extra bytes while a complete frame is written. Serial packetization, radio payload limits, and transmit timing vary by module and must be verified on actual hardware. LoRa is generally half-duplex: applications must coordinate transmissions to avoid radio collisions, and configure acknowledgment timeouts according to the actual air rate, transmission time, and regulatory limits.

### Configuration

Zero values use the following defaults:

| Field | Default | Description |
| --- | --- | --- |
| `MaxFrameSize` | 255 | Maximum complete encoded frame size in bytes; 64–65535 |
| `MaxMessageSize` | 65536 | Maximum sent and received message size; empty messages are not supported |
| `AckTimeout` | 3 seconds | Time to wait for a message-completion acknowledgment after a source stream or repair batch |
| `MaxRetries` | 5 | Maximum additional repair-symbol batches after the initial source-symbol stream |
| `ReceiveTimeout` | 2 minutes | Retention time for an incomplete message without a valid fragment |
| `ReplayWindow` | 10 minutes | Retention time for completed message IDs used for deduplication |

A message can contain at most 56403 RaptorQ source symbols; its actual size limit also depends on `MaxMessageSize` and the fragment size. Larger messages require more encoding computation, memory, and transmission time. The acknowledgment timeout should exceed the time needed to transmit a complete frame over the air and switch the device's direction. The receive timeout and replay window should also cover the longest retry interval.

## Protocol and guarantees

- Frames use COBS encoding with a zero-byte delimiter. The version, type, 128-bit message ID (a random 64-bit link instance ID and 64-bit message ID), symbol ID, original message length, symbol size, and symbol content are protected by HMAC. The receiver verifies this HMAC before passing each source or repair symbol to RaptorQ. Acknowledgment frames are authenticated independently and bound to the message ID and metadata. Corrupt, unauthenticated, or invalid frames are discarded without acknowledgment. The link also ignores its own echoed data frames to avoid mistaking them for peer acknowledgments.
- The sender streams all RaptorQ **source symbols** without waiting for per-symbol acknowledgments. If the message-completion acknowledgment does not arrive, it sends progressively larger batches of new **repair symbols**; it never retransmits an individual source symbol. `MaxRetries` limits these additional repair batches.
- The receiver accepts source and repair symbols in any order, feeds them to the RaptorQ decoder, and sends a single message-completion acknowledgment only after the complete message is decoded and queued. Repeated symbols for an already completed message trigger another completion acknowledgment, allowing recovery from a lost ACK without delivering the message twice. A successful `Send` means the receiving link layer has reassembled and queued the message; it **does not mean the application has read or processed it**. `Receive` should be called continuously to consume messages. When the queue is full, the message is not acknowledged; a later repair symbol retries queueing.
- Each endpoint can handle at most one outgoing and one incoming message at a time; concurrent `Send` calls queue locally. An incoming message canceled or abandoned by its sender occupies the receiver until `ReceiveTimeout`; other messages are not acknowledged during that time.
- Completed message IDs are retained in memory for `ReplayWindow`, so retries with the same ID are not delivered twice. At most 128 unexpired IDs are retained; when the limit is reached, new messages are not acknowledged until an entry expires. This is not persistent replay protection: old messages may be received again after the window expires or the process restarts. A send timeout or cancellation can also occur after the peer has received the message. Applications requiring exactly-once processing across restarts must maintain persistent message IDs and deduplication themselves.
- HMAC provides authentication between holders of the shared key and fragment integrity. It does not provide encryption, key exchange, identity separation between key holders, or application-layer integrity for the complete message. Do not share a key among mutually untrusted nodes. Applications should verify complete messages themselves and encrypt them if needed.

## Testing

```sh
go test -race ./...
```

The tests simulate the link using in-memory duplex streams and lossy packet links; they do not replace serial and radio tests on the actual LoRa module.
