package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// ErrStreamFailed: the server received the stream and reported that handling it failed
// (a NACK), as opposed to the stream or its ACK getting lost.
var ErrStreamFailed = errors.New("server reported failure in stream processing")

// requestFailed is the response-length value a server sends when a request
// handler failed; maxResponseBytes bounds what a client will allocate.
const (
	requestFailed    = ^uint64(0)
	maxResponseBytes = 256 << 20
)

// streamHeader builds the [len][streamType] framing shared by SendStream and
// Request.
func streamHeader(streamType string) ([]byte, error) {
	b := []byte(streamType)
	if len(b) > 255 {
		return nil, fmt.Errorf("stream type identifier too long")
	}
	return append([]byte{byte(len(b))}, b...), nil
}

type tcpClient struct {
	conn net.Conn
	// writeBuf, if > 0, is applied as the kernel send buffer (SO_SNDBUF) of
	// every connection this client opens. A bulk background stream with a big
	// send buffer queues hundreds of KB ahead of anything latency-sensitive
	// sharing the uplink; a small one bounds that queue (M5 pulls).
	writeBuf int
}

// SetWriteBuffer sets the kernel send buffer (bytes) for connections opened
// after the call. Optional: not part of the Client interface.
func (c *tcpClient) SetWriteBuffer(n int) { c.writeBuf = n }

// NewTCPClient creates a new basic TCP transport client
func NewTCPClient() Client {
	return &tcpClient{}
}

func (c *tcpClient) Connect(ctx context.Context, destAddr string) error {
	if c.conn != nil {
		c.conn.Close()
	}

	var err error
	var d net.Dialer
	c.conn, err = d.DialContext(ctx, "tcp", destAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to destination %s: %w", destAddr, err)
	}
	if tcpn, ok := c.conn.(*net.TCPConn); ok && c.writeBuf > 0 {
		_ = tcpn.SetWriteBuffer(c.writeBuf)
	}
	return nil
}

func (c *tcpClient) SendStream(ctx context.Context, streamType string, data io.Reader) error {
	if c.conn == nil {
		return fmt.Errorf("client not connected")
	}

	// A cancelled context ends the transfer now, not at the next deadline (TCP does not notice).
	conn := c.conn
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	// Protocol: [StreamType Length (1 byte)] [StreamType Bytes] [Data...]
	// Using a very simple protocol for the proof-of-concept
	streamTypeBytes := []byte(streamType)
	if len(streamTypeBytes) > 255 {
		return fmt.Errorf("stream type identifier too long")
	}

	header := []byte{byte(len(streamTypeBytes))}
	header = append(header, streamTypeBytes...)

	// Deadlines refresh on every write (idleConn): a transfer making no
	// progress for IdleTimeout fails instead of hanging for ~15 min of TCP
	// retransmissions.
	wconn := idleConn{Conn: c.conn, d: IdleTimeout}
	if _, err := wconn.Write(header); err != nil {
		return fmt.Errorf("failed to write stream header: %w", err)
	}

	// The body is chunk-framed and terminated explicitly (see chunk.go): no
	// half-close, so the reply path never depends on how a NAT in between
	// treats a half-closed connection.
	cw := newChunkWriter(wconn)
	if _, err := io.Copy(cw, data); err != nil {
		return fmt.Errorf("failed to stream data: %w", err)
	}
	if err := cw.Close(); err != nil {
		return fmt.Errorf("failed to terminate stream: %w", err)
	}

	// Wait for ACK from server (reassembly/verify/restore may take a while)
	_ = c.conn.SetReadDeadline(time.Now().Add(AckTimeout))
	ack := make([]byte, 1)
	if _, err := io.ReadFull(c.conn, ack); err != nil {
		return fmt.Errorf("failed to read ACK from server: %w", err)
	}

	if ack[0] != 0x01 {
		return ErrStreamFailed
	}

	return nil
}

// Request opens a dedicated short-lived connection, sends the request body, and
// reads the full response until EOF. It does not disturb any connection held by
// Connect/SendStream — the manifest exchange runs ahead of the migration
// streams.
func (c *tcpClient) Request(ctx context.Context, destAddr, streamType string, body io.Reader) ([]byte, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", destAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to destination %s: %w", destAddr, err)
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	header, err := streamHeader(streamType)
	if err != nil {
		return nil, err
	}
	wconn := idleConn{Conn: conn, d: IdleTimeout}
	if _, err := wconn.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write request header: %w", err)
	}
	cw := newChunkWriter(wconn)
	if body != nil {
		if _, err := io.Copy(cw, body); err != nil {
			return nil, fmt.Errorf("failed to write request body: %w", err)
		}
	}
	if err := cw.Close(); err != nil {
		return nil, fmt.Errorf("failed to terminate request body: %w", err)
	}
	// The response is LENGTH-PREFIXED ([8-byte big-endian length][body]; length
	// MaxUint64 = the handler failed), not delimited by EOF. Reading to EOF hung
	// for the full AckTimeout when an intermediary (QEMU user-mode NAT, measured
	// 2026-10-04) failed to pass the server's FIN through a connection the
	// client had half-closed — the data had arrived, the end-of-stream never did.
	_ = conn.SetReadDeadline(time.Now().Add(AckTimeout))
	var hdr [8]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return nil, fmt.Errorf("failed to read response header: %w", err)
	}
	n := binary.BigEndian.Uint64(hdr[:])
	if n == requestFailed {
		return nil, fmt.Errorf("server failed to serve request %s", streamType)
	}
	if n > maxResponseBytes {
		return nil, fmt.Errorf("response of %d bytes exceeds the %d byte limit", n, uint64(maxResponseBytes))
	}
	resp := make([]byte, n)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, fmt.Errorf("failed to read response body (%d bytes): %w", n, err)
	}
	return resp, nil
}

// OpenConn dials destAddr, writes the [len][streamType] framing (same header
// SendStream/Request use), and returns the live connection — no body, no
// ACK/response read, unlike those two. The caller owns the connection from
// here (protocol, closing) until it's done with it.
func (c *tcpClient) OpenConn(ctx context.Context, destAddr, streamType string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", destAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to destination %s: %w", destAddr, err)
	}
	header, err := streamHeader(streamType)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := conn.Write(header); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to write stream header: %w", err)
	}
	return conn, nil
}

func (c *tcpClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
