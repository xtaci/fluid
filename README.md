# fluid
<img width="1747" height="985" alt="3cea160e4c9c1bf363617d6127a58ea7" src="https://github.com/user-attachments/assets/62c06356-2cc0-48d2-9a99-9735b3a5ccc3" />

A Go link layer for transparent LoRa serial modules. It uses RaptorQ source and repair symbols, authenticates each symbol and acknowledgment with HMAC-SHA-256, and reassembles complete messages for the application.

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
	cfg := fluid.DefaultConfig()
	cfg.Key = key
	link, err := fluid.Open("/dev/ttyUSB0", 9600, cfg)
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

The receiving endpoint opens its own serial port in the same way and calls `message, err := link.Receive(ctx)`. Alternatively, use `New(stream, config)` with any transparent bidirectional stream implementing `io.ReadWriteCloser`. The link owns the stream, and its `Close` method must unblock pending reads and writes. `New` copies `Config.Key` into memguard-protected memory and destroys that copy when the link closes; the caller's original key slice is not modified and remains the caller's responsibility.

### Serial chat

The `cmd/chat` command sends each nonempty input line as a message and
continuously prints incoming messages. Run it on both endpoints with the same
binary key file (at least 32 random bytes), using each endpoint's serial port:

```sh
go run ./cmd/chat -port /dev/ttyUSB0 -baud 9600 -key link.key
# On Windows, for example: go run ./cmd/chat -port COM3 -baud 9600 -key link.key
```

Use `-frame`, `-ack-timeout`, and `-retries` to adjust the link to the radio;
run `go run ./cmd/chat -help` for the defaults. Both endpoints need compatible
frame settings. Ctrl-C or end-of-file exits; end-of-file closes the link after
the last input line is sent, so keep standard input open to continue receiving.
If sending fails, the program exits with an error, but the peer may already
have received the message. The chat is authenticated, **not encrypted**.
On half-duplex radios, take turns sending to avoid collisions.

The context passed to `Send` can cancel waiting for the send lock and acknowledgments, but cannot interrupt a blocked underlying `Write`; close the link to interrupt it. Even when `Send` returns an error, the peer may already have received the message. Messages already queued can still be read with `Receive` after the link closes.

The serial port uses 8N1. Configure the module's frequency, radio parameters, and transparent mode beforehand. This library does not implement vendor-specific AT commands, addressing, transmit/receive switching, or hardware flow control. Both endpoints must use the same key and compatible `MaxWireFrameBytes` and `MaxChunkBytes` settings. `MaxWireFrameBytes` includes the complete serial frame and must not exceed the module's single radio payload limit; the module must not insert extra bytes while a complete frame is written. Serial packetization, radio payload limits, and transmit timing vary by module and must be verified on actual hardware. LoRa is generally half-duplex: applications must coordinate transmissions to avoid radio collisions, and configure acknowledgment timeouts according to the actual air rate, transmission time, and regulatory limits.

### Configuration

`DefaultConfig()` returns a starter configuration for transparent serial LoRa modules. It assumes the module accepts 255-byte wire frames; lower `MaxWireFrameBytes` to the actual radio payload limit if needed. Air rate, spreading factor, coding rate, module turnaround, and regulatory duty-cycle limits vary, so tune `AckTimeout` and application message sizes for the actual link. `MaxAppMessageBytes` limits one complete application message; the library splits it into independently encoded RaptorQ chunks of at most `MaxChunkBytes` and reassembles it before delivery. The defaults are 1 MiB per application message and 32 KiB per RaptorQ chunk. These are upper bounds, not recommendations to send large messages over a low-rate LoRa link. The same values are applied to zero-valued fields when using `Config{Key: key}` directly.

| Field | Default | Description |
| --- | --- | --- |
| `MaxWireFrameBytes` | 255 | Maximum complete encoded wire frame size in bytes, including COBS and delimiter; 76–65535 |
| `MaxAppMessageBytes` | 1 MiB | Maximum complete application message size in bytes; empty messages are not supported |
| `MaxChunkBytes` | 32 KiB | Maximum input bytes for each independent RaptorQ generation |
| `AckTimeout` | 3 seconds | Time to wait for a chunk or message-completion acknowledgment after a source stream or repair batch |
| `MaxRetries` | 5 | Maximum additional repair-symbol batches per chunk after its initial source-symbol stream |
| `ReceiveTimeout` | 2 minutes | Retention time for an incomplete message without a valid fragment |
| `ReplayWindow` | 10 minutes | Retention time for completed message IDs used for deduplication |

Each RaptorQ chunk can contain at most 56403 source symbols; repair symbols use 24-bit encoding symbol IDs. The library automatically reduces a chunk below `MaxChunkBytes` if needed to fit that RaptorQ limit for the configured frame and symbol sizes. Larger application messages require more encoding computation, memory, and transmission time. The acknowledgment timeout should exceed the time needed to transmit a complete frame over the air and switch the device's direction. The receive timeout and replay window should also cover the longest retry interval.

## Protocol and guarantees

- Frames use COBS encoding with a zero-byte delimiter. The version, type, 128-bit message ID (a random 64-bit link instance ID and 64-bit message ID), symbol ID, complete message length, chunk length/index/count, symbol size, and symbol content are protected by HMAC. The receiver verifies this HMAC before passing each source or repair symbol to RaptorQ. Chunk and final acknowledgment frames are authenticated and bound to the message and chunk metadata. Corrupt, unauthenticated, or invalid frames are discarded without acknowledgment. The link also ignores its own echoed data frames to avoid mistaking them for peer acknowledgments.
- The sender splits each application message into ordered chunks. Each chunk gets an independent RaptorQ generation; its source symbols are streamed without per-symbol acknowledgments, followed by progressively larger batches of new repair symbols if needed. A chunk ACK advances to the next generation; the final message-completion ACK is sent only after all chunks are reassembled and queued. `MaxRetries` applies per chunk.
- The receiver accepts source and repair symbols for the active chunk in any order and acknowledges completed non-final chunks so the sender can proceed. It sends a final message-completion acknowledgment only after the complete application message is reassembled and queued. Retries of completed chunks or messages are acknowledged without duplicate delivery. A successful `Send` means the receiving link layer has reassembled and queued the entire message; it **does not mean the application has read or processed it**. `Receive` should be called continuously to consume messages. When the queue is full, the final message is not acknowledged; a later repair symbol retries queueing.
- Each endpoint can handle at most one outgoing and one incoming message at a time; concurrent `Send` calls queue locally. An incoming message canceled or abandoned by its sender occupies the receiver until `ReceiveTimeout`; other messages are not acknowledged during that time.
- Completed message IDs are retained in memory for `ReplayWindow`, so retries with the same ID are not delivered twice. At most 128 unexpired IDs are retained; when the limit is reached, new messages are not acknowledged until an entry expires. This is not persistent replay protection: old messages may be received again after the window expires or the process restarts. A send timeout or cancellation can also occur after the peer has received the message. Applications requiring exactly-once processing across restarts must maintain persistent message IDs and deduplication themselves.
- HMAC provides authentication between holders of the shared key and fragment integrity. It does not provide encryption, key exchange, identity separation between key holders, or application-layer integrity for the complete message. Do not share a key among mutually untrusted nodes. Applications should verify complete messages themselves and encrypt them if needed.

## Testing

```sh
go test -race ./...
```

The tests simulate the link using in-memory duplex streams and lossy packet links; they do not replace serial and radio tests on the actual LoRa module.
