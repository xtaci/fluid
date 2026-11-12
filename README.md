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

### Streaming files and images

`SendStream` and `ReceiveStream` layer a streaming protocol over ordinary
`Link` messages. Files are binary data, so images and other file types work
without special handling. Data is buffered one block/message at a time rather
than loading the whole file into memory. For example, with links opened as
above:

```go
// Sender: src may be an *os.File. The block size is in data bytes.
sent, err := link.SendStream(ctx, src, 64*1024)
// Receiver, running on the other endpoint: dst may be a temporary *os.File.
received, err := link.ReceiveStream(ctx, dst, 1<<30) // accept at most 1 GiB
```

These are separate endpoint calls, not calls to run sequentially on the same
link. Check each error; the returned count is the number of bytes accepted or
acknowledged so far. The block size plus 29 bytes must fit both endpoints'
`MaxAppMessageBytes`; this limit must also be at least 69 bytes for the final
message. Empty files are supported. The receiver checks the complete SHA-256
and sends a confirmation after the writer accepts the data. A successful sender
return confirms that check and write, not filesystem flush or durable storage.
Receive into a temporary file, close/sync it as appropriate, and publish it only
after successful completion; errors can leave partial or unverified output.
If the receiver fails or cancels, the sender can wait for its own context
deadline; cancel both sides as needed. A send error does not prove that the
receiver did not finish. Blocked application reads and writes are not
interrupted by context cancellation. There is no resume or file name metadata.

Reserve **both directions** of a fresh `Link` for one stream transfer; do not
run `Send`, `Receive`, or another stream concurrently. Both peers must
implement this stream protocol and the sender's `ReplayWindow` must be at least
the receiver's. `SendStream` paces transfers after 128 messages to respect the
receiver's replay-ID retention; unrelated previously received messages can
still exhaust that capacity. A long replay window can therefore limit
throughput on fast links. All radio packet-size, buffering, turnaround, and
half-duplex restrictions still apply; large files can take a very long time and
may require smaller blocks, appropriate radio settings, and longer timeouts.

### Ebyte hardware tests

The opt-in tests in `ebyte/` cover raw serial delivery, bidirectional `Link`
messages, and a small PNG transferred and verified in both directions. For two
E90-DTU (400SL37) radios in normal transparent mode (M0 and M1 both ON), using
9600 8N1 and compatible radio settings. The test generates a shared random
link key in memory for both endpoints:

```sh
EBYTE_TEST=1 EBYTE_PORT_A=/dev/cu.usbserial-1130 \
  EBYTE_PORT_B=/dev/cu.usbserial-1110 \
  go test -v -count=1 -timeout 5m ./ebyte -run 'TestHardware(Raw|Link|Image)$'
```

The test uses 240-byte frames and a 10-second ACK timeout. To read registers 00–06
without writing settings, first put **both** radios in configuration mode
(M1 OFF, M0 ON), then run:

```sh
EBYTE_TEST=1 EBYTE_CONFIG_MODE=1 EBYTE_PORT_A=/dev/cu.usbserial-1130 \
  EBYTE_PORT_B=/dev/cu.usbserial-1110 \
  go test -v -count=1 ./ebyte -run '^TestHardwareRegisters$'
```

For a one-minute A-to-B throughput and RaptorQ symbol observation:

```sh
EBYTE_TEST=1 EBYTE_PORT_A=/dev/cu.usbserial-1130 \
  EBYTE_PORT_B=/dev/cu.usbserial-1110 \
  go test -v -count=1 -timeout 3m ./ebyte -run '^TestHardwareThroughput$'
```

The test transmits 512-byte messages and reports application throughput and
authenticated source and repair symbols observed on each serial port. A
difference between transmitted and received symbols can include frames still in
flight at the measurement cutoff; it is not a direct measurement of radio
packet loss. It does not measure link quality for other message sizes or rates.
The test starts new messages for one minute by default and waits for the last
message to finish, so the reported elapsed time may exceed one minute. Use
`EBYTE_DURATION=20s` and `EBYTE_PAYLOAD_BYTES=512` to vary the test. For a
controlled recovery test, set `EBYTE_DROP_FIRST_SOURCE=1` to discard the first
source symbol of every message at the sender before it reaches the radio.
`EBYTE_INITIAL_REPAIRS=1` enables one proactive repair symbol per chunk.
Transmitted counts include the intentionally discarded frames; this models
loss but does not measure radio-layer loss. Compare runs with and without
proactive repairs on the actual device: extra frames can increase buffering,
airtime and half-duplex collisions even on a clean link.

Switch both radios back to normal mode before running communication tests.
The register test cannot compare write-only radio keys. Hardware tests use real
devices and transmit over the air; install antennas, observe local radio rules,
and ensure the ports are not in use. They are skipped unless explicitly enabled.

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
| `InitialRepairSymbols` | 0 | Maximum proactive repair symbols sent after each source stream before waiting for an ACK |
| `ReceiveTimeout` | 2 minutes | Retention time for an incomplete message without a valid fragment |
| `ReplayWindow` | 10 minutes | Retention time for completed message IDs used for deduplication |

Each RaptorQ chunk can contain at most 56403 source symbols; repair symbols use 24-bit encoding symbol IDs. The library automatically reduces a chunk below `MaxChunkBytes` if needed to fit that RaptorQ limit for the configured frame and symbol sizes. Larger application messages require more encoding computation, memory, and transmission time. The acknowledgment timeout should exceed the time needed to transmit a complete frame over the air and switch the device's direction. The receive timeout and replay window should also cover the longest retry interval.

## Protocol and guarantees

- Frames use COBS encoding with a zero-byte delimiter. The version, type, 128-bit message ID (a random 64-bit link instance ID and 64-bit message ID), symbol ID, complete message length, chunk length/index/count, symbol size, and symbol content are protected by HMAC. The receiver verifies this HMAC before passing each source or repair symbol to RaptorQ. Chunk and final acknowledgment frames are authenticated and bound to the message and chunk metadata. Corrupt, unauthenticated, or invalid frames are discarded without acknowledgment. The link also ignores its own echoed data frames to avoid mistaking them for peer acknowledgments.
- The sender splits each application message into ordered chunks. Each chunk gets an independent RaptorQ generation; its source symbols are streamed without per-symbol acknowledgments, followed by progressively larger batches of new repair symbols if needed. A chunk ACK advances to the next generation; the final message-completion ACK is sent only after all chunks are reassembled and queued. `MaxRetries` applies per chunk.
- Optionally, `InitialRepairSymbols` sends repair symbols before the first ACK wait. This can reduce timeout latency when symbols are lost, but adds traffic and can cause collisions or overflow radio buffers. It is disabled by default and does not change the wire format or need matching configuration at the receiver. The sender may stop early upon receiving an ACK; later repair batches use fresh symbol IDs.
- The receiver accepts source and repair symbols for the active chunk in any order and acknowledges completed non-final chunks so the sender can proceed. It sends a final message-completion acknowledgment only after the complete application message is reassembled and queued. Retries of completed chunks or messages are acknowledged without duplicate delivery. A successful `Send` means the receiving link layer has reassembled and queued the entire message; it **does not mean the application has read or processed it**. `Receive` should be called continuously to consume messages. When the queue is full, the final message is not acknowledged; a later repair symbol retries queueing.
- Each endpoint can handle at most one outgoing and one incoming message at a time; concurrent `Send` calls queue locally. An incoming message canceled or abandoned by its sender occupies the receiver until `ReceiveTimeout`; other messages are not acknowledged during that time.
- Completed message IDs are retained in memory for `ReplayWindow`, so retries with the same ID are not delivered twice. At most 128 unexpired IDs are retained; when the limit is reached, new messages are not acknowledged until an entry expires. This is not persistent replay protection: old messages may be received again after the window expires or the process restarts. A send timeout or cancellation can also occur after the peer has received the message. Applications requiring exactly-once processing across restarts must maintain persistent message IDs and deduplication themselves.
- HMAC provides authentication between holders of the shared key and fragment integrity. It does not provide encryption, key exchange, identity separation between key holders, or application-layer integrity for the complete message. Do not share a key among mutually untrusted nodes. Applications should verify complete messages themselves and encrypt them if needed.

## Testing

```sh
go test -race ./...
```

The tests simulate the link using in-memory duplex streams and lossy packet links; they do not replace serial and radio tests on the actual LoRa module.
