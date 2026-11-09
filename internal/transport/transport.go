package transport

import (
	"context"
	"io"
	"net"
)

// Client represents the source node that connects to a destination to send migration data
type Client interface {
	// Connect establishes a connection to the destination
	Connect(ctx context.Context, destAddr string) error

	// SendStream sends a data stream (e.g., memory pages, filesystem) to the destination
	SendStream(ctx context.Context, streamType string, data io.Reader) error

	// Request sends a request body and reads the destination's full response
	// (until EOF), on its own short-lived connection. Used for the pre-burst
	// manifest exchange: the source asks the destination for the
	// page-hash manifest of an image digest and gets the manifest bytes back.
	Request(ctx context.Context, destAddr, streamType string, body io.Reader) ([]byte, error)

	// OpenConn dials destAddr, writes the streamType framing, and hands back
	// the raw connection for the caller to drive a bidirectional protocol of
	// its own — unlike SendStream/Request, the connection stays open until
	// the caller closes it. Used by M5's drain channel (
	// Task 3): the destination cannot dial the source back
	// (NAT'd testbed, no inbound path), so the source opens one long-lived
	// outbound connection for the whole drain window and the destination's
	// pull requests + drain-complete signal ride it in the reverse direction.
	OpenConn(ctx context.Context, destAddr, streamType string) (net.Conn, error)

	// Close terminates the connection
	Close() error
}

// Server represents the destination node that receives migration data
type Server interface {
	// Start begins listening for incoming migration connections
	Start(ctx context.Context, listenAddr string) error

	// ReceiveStream registers a handler for incoming streams of a specific type
	ReceiveStream(streamType string, handler func(io.Reader) error)

	// ReceiveRequest registers a responder for a request/response streamType:
	// the handler reads the request body and returns response bytes, which are
	// written back to the client (instead of the 1-byte ACK of ReceiveStream).
	ReceiveRequest(streamType string, handler func(req io.Reader) ([]byte, error))

	// ReceiveConn registers a handler that takes ownership of the raw
	// connection for a streamType opened via Client.OpenConn — no framing,
	// no ACK, the handler drives its own protocol until it returns (the
	// connection is closed automatically after, same as the other handlers).
	ReceiveConn(streamType string, handler func(net.Conn) error)

	// Stop gracefully shuts down the server
	Stop() error
}
