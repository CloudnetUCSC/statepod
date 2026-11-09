package container

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeRedis accepts connections and answers inline PING with +PONG (or a
// wrong reply when pong is false).
func fakeRedis(t *testing.T, pong bool) net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				line, err := bufio.NewReader(c).ReadString('\n')
				if err != nil || !strings.HasPrefix(line, "PING") {
					return
				}
				if pong {
					c.Write([]byte("+PONG\r\n"))
				} else {
					c.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
				}
			}(conn)
		}
	}()
	return ln
}

func TestWaitReadyNodeListening(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	if _, err := waitReadyAddr(context.Background(), ln.Addr().String(), false, 2*time.Second); err != nil {
		t.Errorf("expected listening socket to be ready, got: %v", err)
	}
}

func TestWaitReadyRedisPong(t *testing.T) {
	ln := fakeRedis(t, true)
	defer ln.Close()

	if _, err := waitReadyAddr(context.Background(), ln.Addr().String(), true, 2*time.Second); err != nil {
		t.Errorf("expected +PONG server to be ready, got: %v", err)
	}
}

func TestWaitReadyRedisNotPong(t *testing.T) {
	ln := fakeRedis(t, false)
	defer ln.Close()

	if _, err := waitReadyAddr(context.Background(), ln.Addr().String(), true, 300*time.Millisecond); err == nil {
		t.Error("expected timeout when the server never answers +PONG")
	}
}

func TestWaitReadyTimeoutNoListener(t *testing.T) {
	// Reserve a port, then close it so nothing is listening there.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	waited, err := waitReadyAddr(context.Background(), addr, false, 300*time.Millisecond)
	if err == nil {
		t.Error("expected error when nothing listens")
	}
	if waited < 300*time.Millisecond {
		t.Errorf("returned before the timeout elapsed (%s)", waited)
	}
}
