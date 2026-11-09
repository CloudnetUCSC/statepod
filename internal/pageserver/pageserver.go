// Package pageserver implements the server side of CRIU's page-server wire
// protocol (criu/criu/page-xfer.c) for exactly the subset M5
// needs: answering a lazy-pages daemon's PS_IOV_GET requests for pages this
// host doesn't have locally (the residual set), fetched via Fetch. This is
// the "fake page server" from code_map.md M5 point 3 — CRIU itself never
// runs as this server; statepod does, bridging CRIU's binary protocol to
// whatever the residual set actually needs (a still-frozen source over the
// WAN, or a local cache once a page has been fetched once).
//
// Wire format (verified against criu/criu/page-xfer.c and
// criu/criu/include/page-xfer.h, and against the actual `criu lazy-pages`
// client live on the testbed VMs — ):
//
//	struct page_server_iov { u32 cmd; u64 nr_pages; u64 vaddr; u64 dst_id; }
//
// 32 bytes on the wire (4 B padding after cmd for u64 alignment), host byte
// order. PS_IOV_OPEN2 opens a session (dst_id encodes the pagemap image);
// this fake server always replies with has_parent=0 (1 byte) — CRIU's own
// generate_iovs()/dump-side use of has_parent doesn't apply here, since the
// in_parent resolution for baseline hits already happened locally via the
// real `parent` symlink (BuildLazyRestoreDir), entirely independent of this
// connection. PS_IOV_GET requests nr_pages at vaddr; the reply is a
// PS_IOV_ADD_F header (cmd upper 16 bits carry PE_PRESENT) immediately
// followed by nr_pages*4096 raw bytes — no further framing, matching CRIU's
// own reference server (page_server_get_pages, page-xfer.c:1138).
package pageserver

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	psIovOpen2 = 4
	psIovAddF  = 6
	psIovGet   = 7

	psIovClose      = 0x1023
	psIovForceClose = 0x1024

	psCmdBits = 16
	psCmdMask = (1 << psCmdBits) - 1

	pePresent uint32 = 1 << 2

	// PageSize is CRIU's page granularity on x86-64 (mirrors pipeline.PageSize;
	// not imported to keep this package dependency-free of internal/pipeline).
	PageSize = 4096

	psIOVWireSize = 32
)

// Fetch resolves nr pages starting at vaddr in the task named by dstID (CRIU's
// encoded pagemap id) to their raw bytes (nr*PageSize).
// Called synchronously per PS_IOV_GET — the caller (a background pusher,
// Task 3, or a direct WAN pull) is responsible for actually reaching the
// frozen source; this package only speaks CRIU's wire protocol.
type Fetch func(dstID, vaddr, nrPages uint64) ([]byte, error)

// Server answers a `criu lazy-pages --page-server` client's requests.
type Server struct {
	Fetch Fetch
}

// ListenAndServe starts accepting connections on addr (host:port; port 0
// picks a free port) and serves them in the background. Returns the bound
// address and a func to stop listening (in-flight connections finish on
// their own).
func (s *Server) ListenAndServe(addr string) (net.Addr, func() error, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	go s.acceptLoop(ln)
	return ln.Addr(), ln.Close, nil
}

func (s *Server) acceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		req, err := readPSIOV(conn)
		if err != nil {
			return
		}
		switch req.Cmd & psCmdMask {
		case psIovOpen2:
			if _, err := conn.Write([]byte{0}); err != nil {
				return
			}
		case psIovGet:
			data, err := s.Fetch(req.DstID, req.Vaddr, req.NrPages)
			if err != nil || uint64(len(data)) != req.NrPages*PageSize {
				// Dropping the connection makes CRIU's restore fail; say why (the daemon
				// only reports "remote side closed connection").
				fmt.Printf("pageserver: dropping connection: GET dst_id=0x%x vaddr=0x%x n=%d: fetch error %v, got %d bytes\n",
					req.DstID, req.Vaddr, req.NrPages, err, len(data))
				return
			}
			reply := psIOV{
				Cmd:     pePresent<<psCmdBits | psIovAddF,
				NrPages: req.NrPages,
				Vaddr:   req.Vaddr,
				DstID:   req.DstID,
			}
			if err := writePSIOV(conn, reply); err != nil {
				return
			}
			if _, err := conn.Write(data); err != nil {
				return
			}
		case psIovClose, psIovForceClose:
			return
		default:
			return
		}
	}
}

// psIOV mirrors CRIU's struct page_server_iov (page-xfer.c:33).
type psIOV struct {
	Cmd     uint32
	NrPages uint64
	Vaddr   uint64
	DstID   uint64
}

func readPSIOV(r io.Reader) (psIOV, error) {
	var buf [psIOVWireSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return psIOV{}, err
	}
	return psIOV{
		Cmd:     binary.LittleEndian.Uint32(buf[0:4]),
		NrPages: binary.LittleEndian.Uint64(buf[8:16]),
		Vaddr:   binary.LittleEndian.Uint64(buf[16:24]),
		DstID:   binary.LittleEndian.Uint64(buf[24:32]),
	}, nil
}

func writePSIOV(w io.Writer, v psIOV) error {
	var buf [psIOVWireSize]byte
	binary.LittleEndian.PutUint32(buf[0:4], v.Cmd)
	binary.LittleEndian.PutUint64(buf[8:16], v.NrPages)
	binary.LittleEndian.PutUint64(buf[16:24], v.Vaddr)
	binary.LittleEndian.PutUint64(buf[24:32], v.DstID)
	n, err := w.Write(buf[:])
	if err == nil && n != psIOVWireSize {
		return fmt.Errorf("short write: %d/%d", n, psIOVWireSize)
	}
	return err
}
