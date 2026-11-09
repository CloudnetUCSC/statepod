package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// freeAddr returns a currently free loopback address.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// Request responses are length-prefixed, so a client never waits for EOF to
// know a response is complete (an intermediary that drops the server's FIN
// must not be able to stall it), and a handler error is a distinct, explicit
// failure rather than an empty body.
func TestRequestResponseFraming(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := freeAddr(t)

	srv := NewTCPServer()
	srv.ReceiveRequest("echo", func(r io.Reader) ([]byte, error) {
		b, err := io.ReadAll(r)
		return append([]byte("re:"), b...), err
	})
	srv.ReceiveRequest("empty", func(r io.Reader) ([]byte, error) {
		_, _ = io.ReadAll(r)
		return nil, nil
	})
	srv.ReceiveRequest("boom", func(r io.Reader) ([]byte, error) {
		_, _ = io.ReadAll(r)
		return nil, errors.New("handler exploded")
	})
	go func() { _ = srv.Start(ctx, addr) }()
	defer srv.Stop()
	time.Sleep(100 * time.Millisecond)

	c := NewTCPClient()
	defer c.Close()

	got, err := c.Request(ctx, addr, "echo", strings.NewReader("hello"))
	if err != nil || string(got) != "re:hello" {
		t.Fatalf("echo: got %q err %v", got, err)
	}

	big := bytes.Repeat([]byte("x"), 3<<20)
	got, err = c.Request(ctx, addr, "echo", bytes.NewReader(big))
	if err != nil || len(got) != len(big)+3 {
		t.Fatalf("large echo: len %d err %v", len(got), err)
	}

	got, err = c.Request(ctx, addr, "empty", nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty response must be a success with no bytes: got %q err %v", got, err)
	}

	if _, err = c.Request(ctx, addr, "boom", strings.NewReader("x")); err == nil {
		t.Fatal("handler error must surface as a client error, not an empty success")
	}
}
