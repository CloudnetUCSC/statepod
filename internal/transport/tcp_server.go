package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type tcpServer struct {
	listener     net.Listener
	handlers     map[string]func(io.Reader) error
	responders   map[string]func(io.Reader) ([]byte, error)
	connHandlers map[string]func(net.Conn) error
	mu           sync.RWMutex
}

// NewTCPServer creates a new basic TCP transport server
func NewTCPServer() Server {
	return &tcpServer{
		handlers:     make(map[string]func(io.Reader) error),
		responders:   make(map[string]func(io.Reader) ([]byte, error)),
		connHandlers: make(map[string]func(net.Conn) error),
	}
}

func (s *tcpServer) Start(ctx context.Context, listenAddr string) error {
	var err error
	s.listener, err = net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	log.Printf("TCP Transport Server listening on %s\n", listenAddr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil // Shutdown requested
			default:
				// If listener is closed via Stop(), Accept will return an error
				return err
			}
		}
		go s.handleConnection(conn)
	}
}

func (s *tcpServer) ReceiveStream(streamType string, handler func(io.Reader) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[streamType] = handler
}

func (s *tcpServer) ReceiveRequest(streamType string, handler func(io.Reader) ([]byte, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responders[streamType] = handler
}

func (s *tcpServer) ReceiveConn(streamType string, handler func(net.Conn) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connHandlers[streamType] = handler
}

func (s *tcpServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Header + payload reads and the ACK write refresh their deadline on
	// every operation (see IdleTimeout); raw conn handlers below get the
	// bare connection since the drain channel is idle by design.
	raw := conn
	conn = idleConn{Conn: raw, d: IdleTimeout}

	// Read stream type header length
	headerLen := make([]byte, 1)
	if _, err := io.ReadFull(conn, headerLen); err != nil {
		if err != io.EOF {
			log.Printf("Failed to read header length: %v\n", err)
		}
		return
	}

	// Read stream type
	streamTypeBytes := make([]byte, headerLen[0])
	if _, err := io.ReadFull(conn, streamTypeBytes); err != nil {
		log.Printf("Failed to read stream type: %v\n", err)
		return
	}

	streamType := string(streamTypeBytes)

	s.mu.RLock()
	handler, exists := s.handlers[streamType]
	responder, isResponder := s.responders[streamType]
	connHandler, isConnHandler := s.connHandlers[streamType]
	s.mu.RUnlock()

	// Raw connection streamType (Client.OpenConn): the handler owns the
	// connection until it returns — no framing, no ACK/response write here.
	// The deferred conn.Close() above still applies once it does.
	if isConnHandler {
		_ = raw.SetDeadline(time.Time{})
		if err := connHandler(raw); err != nil {
			log.Printf("Error in conn handler %s: %v\n", streamType, err)
		}
		return
	}

	// Request/response streamType: read the request body, write the response
	// bytes back (no ACK byte — the client reads until EOF).
	if isResponder {
		t0 := time.Now()
		resp, rerr := responder(newChunkReader(conn))
		var hdr [8]byte
		if rerr != nil {
			log.Printf("Error serving request %s: %v (after %.1fs)\n", streamType, rerr, time.Since(t0).Seconds())
			binary.BigEndian.PutUint64(hdr[:], requestFailed)
			_, _ = conn.Write(hdr[:]) // explicit failure marker; the client does not wait for EOF
			return
		}
		binary.BigEndian.PutUint64(hdr[:], uint64(len(resp)))
		_, werr := conn.Write(append(hdr[:], resp...))
		if werr != nil {
			log.Printf("Error writing response for %s: %v\n", streamType, werr)
		}
		log.Printf("Request %s served: handler %.1fs, response %d B written (err=%v)\n", streamType, time.Since(t0).Seconds(), len(resp), werr)
		return
	}

	if !exists {
		log.Printf("No handler registered for stream type: %s\n", streamType)
		// Send failure ACK
		conn.Write([]byte{0x00})
		return
	}

	err := handler(newChunkReader(conn))
	if err != nil {
		log.Printf("Error handling stream type %s: %v\n", streamType, err)
		// Send failure ACK
		conn.Write([]byte{0x00})
	} else {
		// Send success ACK
		conn.Write([]byte{0x01})
	}
}

func (s *tcpServer) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
