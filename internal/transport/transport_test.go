package transport

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"
	"time"
)

func TestTCPServerClientIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listenAddr := "127.0.0.1:0" // System assigns port

	server := NewTCPServer()

	// Buffered: the handler must be able to complete (and the server send
	// the ACK the client is waiting on in SendStream) before the test
	// goroutine reads from this channel — unbuffered would deadlock.
	receiveCh := make(chan string, 1)

	// Register a stream handler
	server.ReceiveStream("test-stream", func(r io.Reader) error {
		data, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}
		receiveCh <- string(data)
		return nil
	})

	// Start server on a specific address. Start blocks in an accept loop
	// (the receiver runs it with `go server.Start(...)`), so run it in a
	// goroutine here too and surface any startup error via a channel.
	listenAddr = "127.0.0.1:8081"
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- server.Start(ctx, listenAddr)
	}()
	defer server.Stop()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// --- Client side ---
	client := NewTCPClient()
	err := client.Connect(ctx, listenAddr)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer client.Close()

	// Send a payload
	payload := "Hello StatePod Migration!"
	err = client.SendStream(ctx, "test-stream", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatalf("Failed to send stream: %v", err)
	}

	// Verify server received it
	select {
	case received := <-receiveCh:
		if received != payload {
			t.Errorf("Expected payload '%s', got '%s'", payload, received)
		}
	case err := <-serverErrCh:
		t.Fatalf("Server exited unexpectedly: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for server to receive the stream")
	}
}
