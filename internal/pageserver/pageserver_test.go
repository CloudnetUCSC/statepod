package pageserver

import (
	"io"
	"net"
	"testing"
)

// TestOpen2ThenGet drives the server exactly the way `criu lazy-pages`'s
// client role does (page-xfer.c: open_page_server_xfer then
// request_remote_pages): PS_IOV_OPEN2 first (expect a 1 B has_parent=0
// reply), then PS_IOV_GET (expect a PS_IOV_ADD_F header + raw bytes).
func TestOpen2ThenGet(t *testing.T) {
	want := make([]byte, 2*PageSize)
	for i := range want {
		want[i] = byte(i)
	}

	srv := &Server{Fetch: func(_, vaddr, nrPages uint64) ([]byte, error) {
		if vaddr != 0x555500000000 || nrPages != 2 {
			t.Fatalf("unexpected fetch(%#x, %d)", vaddr, nrPages)
		}
		return want, nil
	}}
	addr, stop, err := srv.ListenAndServe("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// PS_IOV_OPEN2, dst_id = some encoded pagemap id.
	if err := writePSIOV(conn, psIOV{Cmd: psIovOpen2, DstID: 0x101}); err != nil {
		t.Fatal(err)
	}
	var hasParent [1]byte
	if _, err := io.ReadFull(conn, hasParent[:]); err != nil {
		t.Fatal(err)
	}
	if hasParent[0] != 0 {
		t.Fatalf("has_parent = %d, want 0", hasParent[0])
	}

	// PS_IOV_GET for 2 pages at a fixed vaddr.
	if err := writePSIOV(conn, psIOV{Cmd: psIovGet, NrPages: 2, Vaddr: 0x555500000000, DstID: 0x101}); err != nil {
		t.Fatal(err)
	}
	reply, err := readPSIOV(conn)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Cmd&psCmdMask != psIovAddF {
		t.Fatalf("reply cmd = %#x, want PS_IOV_ADD_F", reply.Cmd&psCmdMask)
	}
	if reply.Cmd>>psCmdBits != pePresent {
		t.Fatalf("reply flags = %#x, want PE_PRESENT", reply.Cmd>>psCmdBits)
	}
	if reply.NrPages != 2 || reply.Vaddr != 0x555500000000 {
		t.Fatalf("reply = %+v", reply)
	}

	got := make([]byte, 2*PageSize)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("byte %d = %d, want %d", i, got[i], want[i])
		}
	}
}

// TestWireSizeIs32Bytes pins the on-wire struct size to what CRIU expects
// (page-xfer.c: 4 B cmd + 4 B padding + 8+8+8 B fields = 32 B).
func TestWireSizeIs32Bytes(t *testing.T) {
	r, w := net.Pipe()
	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		n, err := r.Read(buf)
		if err != nil {
			done <- err
			return
		}
		if n != psIOVWireSize {
			t.Errorf("wrote %d bytes, want %d", n, psIOVWireSize)
		}
		done <- nil
	}()
	if err := writePSIOV(w, psIOV{Cmd: psIovGet}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
