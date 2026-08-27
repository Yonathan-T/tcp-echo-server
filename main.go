package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type ServerStats struct {
	TotalConnections  int64 `json:"total_connections"`
	ActiveConnections int64 `json:"active_connections"`
	TotalBytesEchoed  int64 `json:"total_bytes_echoed"`
}

type Config struct {
	Addr        string
	BufferSize  int
	IdleTimeout time.Duration
}

type Server struct {
	cfg Config

	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	running bool

	totalConnections  int64
	activeConnections int64
	totalBytesEchoed  int64
}

func NewServer(cfg Config) *Server {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 4096
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
		conns:  make(map[net.Conn]struct{}),
	}
}

func (s *Server) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("server is already running")
	}

	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to listen on %s: %w", s.cfg.Addr, err)
	}

	s.listener = ln
	s.running = true
	s.cfg.Addr = ln.Addr().String()
	s.mu.Unlock()

	log.Printf("[INFO] TCP Echo Server listening on %s", s.cfg.Addr)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.cfg.Addr
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				log.Printf("[WARN] Accept error: %v", err)
				continue
			}
		}

		s.registerConn(conn)
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConn(c)
		}(conn)
	}
}

func (s *Server) registerConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[conn] = struct{}{}
	atomic.AddInt64(&s.totalConnections, 1)
	atomic.AddInt64(&s.activeConnections, 1)
}

func (s *Server) unregisterConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.conns[conn]; ok {
		delete(s.conns, conn)
		atomic.AddInt64(&s.activeConnections, -1)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		s.unregisterConn(conn)
		conn.Close()
	}()

	remoteAddr := conn.RemoteAddr().String()
	log.Printf("[DEBUG] Accepted connection from %s", remoteAddr)

	buf := make([]byte, s.cfg.BufferSize)

	for {
		if s.cfg.IdleTimeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(s.cfg.IdleTimeout))
		}

		n, err := conn.Read(buf)
		if n > 0 {
			written := 0
			for written < n {
				if s.cfg.IdleTimeout > 0 {
					_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.IdleTimeout))
				}
				m, writeErr := conn.Write(buf[written:n])
				if writeErr != nil {
					log.Printf("[DEBUG] Write error to %s: %v", remoteAddr, writeErr)
					return
				}
				written += m
			}
			atomic.AddInt64(&s.totalBytesEchoed, int64(n))
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("[DEBUG] Client %s closed connection", remoteAddr)
			} else {
				select {
				case <-s.ctx.Done():
				default:
					log.Printf("[DEBUG] Read error from %s: %v", remoteAddr, err)
				}
			}
			return
		}
	}
}

func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.cancel()

	var closeErr error
	if s.listener != nil {
		closeErr = s.listener.Close()
	}

	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[INFO] Server stopped gracefully")
		return closeErr
	case <-ctx.Done():
		log.Println("[WARN] Shutdown timed out, forcing exit")
		return ctx.Err()
	}
}

func (s *Server) Stats() ServerStats {
	return ServerStats{
		TotalConnections:  atomic.LoadInt64(&s.totalConnections),
		ActiveConnections: atomic.LoadInt64(&s.activeConnections),
		TotalBytesEchoed:  atomic.LoadInt64(&s.totalBytesEchoed),
	}
}

func main() {
	port := flag.Int("port", 8080, "Port to listen on")
	host := flag.String("host", "0.0.0.0", "Host address to bind to")
	bufSize := flag.Int("buf", 4096, "Buffer size in bytes for reading/writing")
	idleTimeout := flag.Duration("idle-timeout", 0, "Idle timeout for connections (e.g. 30s, 1m). 0 disables timeout")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)
	srv := NewServer(Config{
		Addr:        addr,
		BufferSize:  *bufSize,
		IdleTimeout: *idleTimeout,
	})

	if err := srv.Start(); err != nil {
		log.Fatalf("[FATAL] Could not start server: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("[INFO] Received signal %v, shutting down...", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Stop(shutdownCtx); err != nil {
		log.Printf("[ERROR] Error during shutdown: %v", err)
	}

	stats := srv.Stats()
	log.Printf("[INFO] Final Stats - Total Connections: %d, Total Bytes Echoed: %d",
		stats.TotalConnections, stats.TotalBytesEchoed)
}
