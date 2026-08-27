package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func runInteractive(addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", addr, err)
	}
	defer conn.Close()

	fmt.Printf("Connected to TCP Echo Server at %s\nType messages below and press Enter (Ctrl+C to quit):\n\n", addr)

	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				if err != io.EOF {
					fmt.Printf("\n[Disconnected: %v]\n", err)
				} else {
					fmt.Println("\n[Server closed connection]")
				}
				os.Exit(0)
			}
			fmt.Printf("<-- Echo: %s", string(buf[:n]))
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text() + "\n"
		_, err := conn.Write([]byte(text))
		if err != nil {
			log.Fatalf("Failed to send: %v", err)
		}
	}
}

func runBenchmark(addr string, concurrency, requestsPerClient, payloadSize int) {
	fmt.Println("=== TCP Echo Server Benchmark ===")
	fmt.Printf("Target:            %s\n", addr)
	fmt.Printf("Concurrency:       %d workers\n", concurrency)
	fmt.Printf("Requests / Worker: %d\n", requestsPerClient)
	fmt.Printf("Total Requests:    %d\n", concurrency*requestsPerClient)
	fmt.Printf("Payload Size:      %d bytes\n\n", payloadSize)

	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		log.Fatalf("Failed to generate test payload: %v", err)
	}

	var (
		totalCompleted int64
		totalErrors    int64
		totalLatencyNs int64
		minLatencyNs   int64 = 1<<62 - 1
		maxLatencyNs   int64
		wg             sync.WaitGroup
	)

	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				atomic.AddInt64(&totalErrors, int64(requestsPerClient))
				return
			}
			defer conn.Close()

			recvBuf := make([]byte, payloadSize)

			for req := 0; req < requestsPerClient; req++ {
				reqStart := time.Now()

				if _, err := conn.Write(payload); err != nil {
					atomic.AddInt64(&totalErrors, 1)
					return
				}

				if _, err := io.ReadFull(conn, recvBuf); err != nil {
					atomic.AddInt64(&totalErrors, 1)
					return
				}

				if !bytes.Equal(payload, recvBuf) {
					atomic.AddInt64(&totalErrors, 1)
					return
				}

				dur := time.Since(reqStart).Nanoseconds()
				atomic.AddInt64(&totalLatencyNs, dur)
				atomic.AddInt64(&totalCompleted, 1)

				for {
					curMin := atomic.LoadInt64(&minLatencyNs)
					if dur >= curMin || atomic.CompareAndSwapInt64(&minLatencyNs, curMin, dur) {
						break
					}
				}
				for {
					curMax := atomic.LoadInt64(&maxLatencyNs)
					if dur <= curMax || atomic.CompareAndSwapInt64(&maxLatencyNs, curMax, dur) {
						break
					}
				}
			}
		}(i)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	completed := atomic.LoadInt64(&totalCompleted)
	errs := atomic.LoadInt64(&totalErrors)

	fmt.Println("=== Results ===")
	fmt.Printf("Elapsed Time:      %v\n", totalDuration)
	fmt.Printf("Completed:         %d\n", completed)
	fmt.Printf("Errors:            %d\n", errs)

	if completed > 0 {
		avgLatency := time.Duration(atomic.LoadInt64(&totalLatencyNs) / completed)
		minLat := time.Duration(atomic.LoadInt64(&minLatencyNs))
		maxLat := time.Duration(atomic.LoadInt64(&maxLatencyNs))
		rps := float64(completed) / totalDuration.Seconds()
		throughputMB := (float64(completed*int64(payloadSize)*2) / (1024 * 1024)) / totalDuration.Seconds()

		fmt.Printf("Throughput (RPS):  %.2f req/s\n", rps)
		fmt.Printf("Bandwidth (I/O):   %.2f MB/s\n", throughputMB)
		fmt.Printf("Min Latency:       %v\n", minLat)
		fmt.Printf("Avg Latency:       %v\n", avgLatency)
		fmt.Printf("Max Latency:       %v\n", maxLat)
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "Server address to connect to")
	bench := flag.Bool("bench", false, "Run in benchmark mode")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers in benchmark mode")
	requests := flag.Int("requests", 100, "Requests per worker in benchmark mode")
	payloadSize := flag.Int("size", 256, "Payload size in bytes per message")
	flag.Parse()

	if *bench {
		runBenchmark(*addr, *concurrency, *requests, *payloadSize)
	} else {
		runInteractive(*addr)
	}
}
