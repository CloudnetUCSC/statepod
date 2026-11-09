package transport

import (
	"net"
	"time"
)

// IdleTimeout bounds how long a transfer may make NO progress before the
// operation fails: every Read/Write on a framed transport connection gets a
// fresh deadline of this length. Without it a connection that silently stops
// delivering (a NAT/middlebox dropping state, a WAN blackout) blocks until TCP
// retransmission gives up (~15 min), and the migration's own retry/fallback
// logic never gets to run. Generous on purpose: on the 0.3-5 Mbit/s
// home link a 32 KiB chunk still moves in well under a second.
var IdleTimeout = 120 * time.Second

// AckTimeout bounds the wait for the receiver's ACK after a stream's last
// byte: the receiver may legitimately spend a while reassembling, verifying a
// checksum and (for the restore triggers) restoring the container.
var AckTimeout = 10 * time.Minute

// idleConn refreshes the connection deadline before every Read and Write.
// It is NOT applied to raw connections handed to OpenConn/ReceiveConn
// callers (the M5 drain channel is long-lived and idle by design).
type idleConn struct {
	net.Conn
	d time.Duration
}

func (c idleConn) Read(p []byte) (int, error) {
	_ = c.Conn.SetReadDeadline(time.Now().Add(c.d))
	return c.Conn.Read(p)
}

func (c idleConn) Write(p []byte) (int, error) {
	_ = c.Conn.SetWriteDeadline(time.Now().Add(c.d))
	return c.Conn.Write(p)
}
