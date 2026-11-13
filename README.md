# fluid

<img width="1362" height="768" alt="ebyte" src="https://github.com/user-attachments/assets/54e8bcae-ed10-4a23-9745-aa75df9cb9eb" />

A Go transport for transparent LoRa serial modules. It provides a bounded,
coordinated bidirectional byte stream over an authenticated RaptorQ message
link. Frames use HMAC-MD5; its 16-byte tag reduces overhead compared with
HMAC-SHA-256, but MD5 is a legacy hash and has a lower security margin.

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
	message, err := fluid.Open("/dev/ttyUSB0", 9600, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer message.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := message.Send(ctx, []byte("hello LoRa")); err != nil {
		log.Fatal(err)
	}
}
```

The receiving endpoint opens its own serial port in the same way and calls `data, err := message.Receive(ctx)`. Alternatively, use `New(stream, config)` with any transparent bidirectional stream implementing `io.ReadWriteCloser`. The `Message` transport owns the stream, and its `Close` method must unblock pending reads and writes. `New` copies `Config.Key` into memguard-protected memory and destroys that copy when the transport closes; the caller's original key slice is not modified and remains the caller's responsibility.

### Bidirectional byte streams

`Session` implements `io.ReadWriteCloser` on a fresh, exclusively owned `Message`.
Create exactly one primary and one secondary; the primary polls and the
secondary replies. Both directions can be read and written concurrently.
For example, after opening a message transport on each endpoint:

```go
cfg := fluid.DefaultSessionConfig()
cfg.Primary = true // false on the other endpoint
session, err := fluid.NewSession(message, cfg)
if err != nil {
	log.Fatal(err)
}
defer session.Close()
// Use session.Read and session.Write concurrently as needed.
```

`Write` accepts bytes into a bounded local buffer; it does not wait for remote
delivery. `Flush(ctx)` waits for writes accepted before the call to be queued
by the peer message transport, not for the peer application to read or persist them. `Close`
is an abort: it closes the underlying transport, unblocks local calls, and may discard
buffered data. Flush before closing if delivery is required. There is no
graceful EOF, half-close, persistence or restart/resume; transport failures are
reported to blocked and future operations, and a disconnected peer can be
detected only after an exchange times out. `Read` drains already buffered
bytes before returning a terminal error.

The default idle poll interval is five seconds, but actual latency also includes
airtime, ACKs, retransmissions and replay pacing. Primary and secondary take
turns at the message level; this cannot guarantee collision-free operation on
every radio, because serial writes may be buffered after they return. Configure
radio packet size, timing, flow control and duty-cycle compliance for the
hardware. One peer must be primary; both peers must use the session protocol and
compatible `Message` configuration. `PollInterval`, `ExchangeTimeout`,
`BufferBytes` and `BlockBytes` are configurable independently at each end.
Buffer sizes are per direction; block size plus 42 bytes must fit the peer's
`MaxAppMessageBytes`. Do not call other `Message` methods while Session owns it.

The current implementation uses `Message` transmissions for polls and payloads and
paces after 128 messages based on the peer's advertised replay window. Idle
polls also consume message IDs. This can cause long stalls and restrict
throughput, particularly with the default 10-minute replay window. Use a fresh
message transport, set timeouts and replay windows for the actual radio retry duration, and
do not treat this implementation as a replacement for durable delivery or
persistent replay protection. HMAC authenticates traffic but does not encrypt.

### Ebyte hardware tests

The opt-in tests in `ebyte/` cover raw serial delivery, bidirectional `Message`
transfers, and a session. For two
E90-DTU (400SL37) radios in normal transparent mode (M0 and M1 both ON), using
9600 8N1 and compatible radio settings. The test generates a shared random
link key in memory for both endpoints:

```sh
EBYTE_TEST=1 EBYTE_PORT_A=/dev/cu.usbserial-1130 \
  EBYTE_PORT_B=/dev/cu.usbserial-1110 \
  go test -v -count=1 -timeout 5m ./ebyte -run 'TestHardware(Raw|Message|Session)$'
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
`EBYTE_INITIAL_REPAIRS=1` enables one proactive repair symbol per message.
Transmitted counts include the intentionally discarded frames; this models
loss but does not measure radio-layer loss. Compare runs with and without
proactive repairs on the actual device: extra frames can increase buffering,
airtime and half-duplex collisions even on a clean link.

Switch both radios back to normal mode before running communication tests.
The register test cannot compare write-only radio keys. Hardware tests use real
devices and transmit over the air; install antennas, observe local radio rules,
and ensure the ports are not in use. They are skipped unless explicitly enabled.

The context passed to `Send` can cancel waiting for the send lock and acknowledgments, but cannot interrupt a blocked underlying `Write`; close the link to interrupt it. Even when `Send` returns an error, the peer may already have received the message. Messages already queued can still be read with `Receive` after the link closes.

The serial port uses 8N1. Configure the module's frequency, radio parameters, and transparent mode beforehand. This library does not implement vendor-specific AT commands, addressing, transmit/receive switching, or hardware flow control. Both endpoints must use the same key and compatible `MaxWireFrameBytes` and `MaxAppMessageBytes` settings. `MaxWireFrameBytes` includes the complete serial frame and must not exceed the module's single radio payload limit; the module must not insert extra bytes while a complete frame is written. Serial packetization, radio payload limits, and transmit timing vary by module and must be verified on actual hardware. LoRa is generally half-duplex: applications must coordinate transmissions to avoid radio collisions, and configure acknowledgment timeouts according to the actual air rate, transmission time, and regulatory limits.

### Configuration

`DefaultConfig()` returns a starter configuration for transparent serial LoRa modules. It assumes the module accepts 255-byte wire frames; lower `MaxWireFrameBytes` to the actual radio payload limit if needed. Air rate, spreading factor, coding rate, module turnaround, and regulatory duty-cycle limits vary, so tune `AckTimeout` and application message sizes for the actual link. `MaxAppMessageBytes` limits one complete application message, which is encoded as one RaptorQ generation. Session breaks the byte stream into separate bounded messages using `BlockBytes`. The default 1 MiB message limit is an upper bound, not a recommendation to send large messages over a low-rate LoRa link. The same values are applied to zero-valued fields when using `Config{Key: key}` directly.

| Field | Default | Description |
| --- | --- | --- |
| `MaxWireFrameBytes` | 255 | Maximum complete encoded wire frame size in bytes, including COBS and delimiter; 76–65535 |
| `MaxAppMessageBytes` | 1 MiB | Maximum complete application message size in bytes; empty messages are not supported |
| `AckTimeout` | 3 seconds | Time to wait for a message-completion acknowledgment after a source stream or repair batch |
| `MaxRetries` | 5 | Maximum additional repair-symbol batches per message after its initial source-symbol stream |
| `InitialRepairSymbols` | 0 | Maximum proactive repair symbols sent after each source stream before waiting for an ACK |
| `ReceiveTimeout` | 2 minutes | Retention time for an incomplete message without a valid fragment |
| `ReplayWindow` | 10 minutes | Retention time for completed message IDs used for deduplication |

Each RaptorQ generation can contain at most 56403 source symbols; repair symbols use 24-bit encoding symbol IDs. Messages exceeding that limit for the configured symbol size are rejected. Larger application messages require more encoding computation, memory, and transmission time. The acknowledgment timeout should exceed the time needed to transmit a complete frame over the air and switch the device's direction. The receive timeout and replay window should also cover the longest retry interval.

## Protocol and guarantees

- Frames use COBS encoding with a zero-byte delimiter. The version, type, 128-bit message ID (a random 64-bit link instance ID and 64-bit message ID), symbol ID, complete message length, symbol size, and symbol content are protected by HMAC-MD5. The receiver verifies the 16-byte tag before passing each source or repair symbol to RaptorQ. Message acknowledgment frames are authenticated and bound to the message metadata. Corrupt, unauthenticated, or invalid frames are discarded without acknowledgment. The link also ignores its own echoed data frames to avoid mistaking them for peer acknowledgments.
- Each application message is one independent RaptorQ generation; its source symbols are streamed without per-symbol acknowledgments, followed by progressively larger batches of new repair symbols if needed. The message-completion ACK is sent only after the message is decoded and queued.
- Optionally, `InitialRepairSymbols` sends repair symbols before the first ACK wait. This can reduce timeout latency when symbols are lost, but adds traffic and can cause collisions or overflow radio buffers. It is disabled by default and does not change the wire format or need matching configuration at the receiver. The sender may stop early upon receiving an ACK; later repair batches use fresh symbol IDs.
- The receiver accepts source and repair symbols in any order and sends a message-completion acknowledgment only after the message is decoded and queued. Retries of completed messages are acknowledged without duplicate delivery. A successful `Send` means the receiving link layer has queued the entire message; it **does not mean the application has read or processed it**. `Receive` should be called continuously to consume messages. When the queue is full, the message is not acknowledged; a later repair symbol retries queueing.
- Each endpoint can handle at most one outgoing and one incoming message at a time; concurrent `Send` calls queue locally. An incoming message canceled or abandoned by its sender occupies the receiver until `ReceiveTimeout`; other messages are not acknowledged during that time.
- Completed message IDs are retained in memory for `ReplayWindow`, so retries with the same ID are not delivered twice. At most 128 unexpired IDs are retained; when the limit is reached, new messages are not acknowledged until an entry expires. This is not persistent replay protection: old messages may be received again after the window expires or the process restarts. A send timeout or cancellation can also occur after the peer has received the message. Applications requiring exactly-once processing across restarts must maintain persistent message IDs and deduplication themselves.
- HMAC provides authentication between holders of the shared key and fragment integrity. It does not provide encryption, key exchange, identity separation between key holders, or application-layer integrity for the complete message. Do not share a key among mutually untrusted nodes. Applications should verify complete messages themselves and encrypt them if needed.

## Testing

```sh
go test -race ./...
```

The tests simulate the link using in-memory duplex streams and lossy packet links; they do not replace serial and radio tests on the actual LoRa module.
