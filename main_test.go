package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func startTestServer(t *testing.T, cfg Config) (*Server, func()) {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	srv := NewServer(cfg)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	}

	return srv, cleanup
}

func TestEchoSingleMessage(t *testing.T) {
	srv, cleanup := startTestServer(t, Config{Addr: "127.0.0.1:0"})
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	msg := []byte("Hello, TCP Echo Server!\n")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if !bytes.Equal(msg, buf) {
		t.Fatalf("got %q, want %q", buf, msg)
	}
}

func TestEchoMultipleMessages(t *testing.T) {
	srv, cleanup := startTestServer(t, Config{Addr: "127.0.0.1:0"})
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	messages := []string{
		"first message\n",
		"second message\n",
		"third message with more words\n",
	}

	for _, msgStr := range messages {
		msg := []byte(msgStr)
		if _, err := conn.Write(msg); err != nil {
			t.Fatalf("write failed: %v", err)
		}

		buf := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("read failed: %v", err)
		}

		if !bytes.Equal(msg, buf) {
			t.Fatalf("got %q, want %q", buf, msg)
		}
	}
}

func TestConcurrentConnections(t *testing.T) {
	srv, cleanup := startTestServer(t, Config{Addr: "127.0.0.1:0"})
	defer cleanup()

	numClients := 25
	msgsPerClient := 10
	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", srv.Addr())
			if err != nil {
				t.Errorf("client %d: dial failed: %v", clientID, err)
				return
			}
			defer conn.Close()

			for m := 0; m < msgsPerClient; m++ {
				payload := fmt.Sprintf("client-%d-msg-%d\n", clientID, m)
				if _, err := conn.Write([]byte(payload)); err != nil {
					t.Errorf("client %d: write failed: %v", clientID, err)
					return
				}

				buf := make([]byte, len(payload))
				if _, err := io.ReadFull(conn, buf); err != nil {
					t.Errorf("client %d: read failed: %v", clientID, err)
					return
				}

				if string(buf) != payload {
					t.Errorf("client %d: got %q, want %q", clientID, string(buf), payload)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	stats := srv.Stats()
	if stats.TotalConnections != int64(numClients) {
		t.Errorf("total connections: got %d, want %d", stats.TotalConnections, numClients)
	}
}

func TestLargePayload(t *testing.T) {
	srv, cleanup := startTestServer(t, Config{Addr: "127.0.0.1:0", BufferSize: 8192})
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	payloadSize := 128 * 1024
	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("failed to generate random payload: %v", err)
	}

	readDone := make(chan []byte)
	go func() {
		received := make([]byte, payloadSize)
		_, readErr := io.ReadFull(conn, received)
		if readErr != nil {
			t.Errorf("read full payload failed: %v", readErr)
		}
		readDone <- received
	}()

	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	select {
	case received := <-readDone:
		if !bytes.Equal(payload, received) {
			t.Fatal("large payload corrupted during echo")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for large payload echo")
	}
}

func TestGracefulShutdown(t *testing.T) {
	srv, _ := startTestServer(t, Config{Addr: "127.0.0.1:0"})

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	time.Sleep(20 * time.Millisecond)
	if srv.Stats().ActiveConnections != 1 {
		t.Fatalf("active connections: got %d, want 1", srv.Stats().ActiveConnections)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := srv.Stop(ctx); err != nil {
		t.Fatalf("stop failed: %v", err)
	}

	_, err = net.DialTimeout("tcp", srv.Addr(), 200*time.Millisecond)
	if err == nil {
		t.Fatal("dial succeeded after server stop, expected error")
	}
}

func TestIdleTimeout(t *testing.T) {
	srv, cleanup := startTestServer(t, Config{
		Addr:        "127.0.0.1:0",
		IdleTimeout: 100 * time.Millisecond,
	})
	defer cleanup()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	time.Sleep(250 * time.Millisecond)

	buf := make([]byte, 10)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected error reading from timed-out connection")
	}
}
