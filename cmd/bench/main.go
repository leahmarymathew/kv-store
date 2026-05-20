package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/leahmarymathew/kv-store/internal/protocol"
)

func main() {
	host := flag.String("host", "localhost", "server host")
	port := flag.Int("port", 7379, "server port")
	clients := flag.Int("clients", 50, "number of concurrent clients")
	requests := flag.Int("requests", 10000, "total requests, divided among clients")
	payloadSize := flag.Int("payload-size", 64, "value size in bytes")
	operation := flag.String("operation", "mixed", "operation type: get, set, or mixed")
	warmup := flag.Int("warmup", 1000, "warmup requests before measuring")
	flag.Parse()

	addr := net.JoinHostPort(*host, strconv.Itoa(*port))
	value := bytes.Repeat([]byte("x"), *payloadSize)

	// Warmup phase
	fmt.Printf("Warming up with %d requests...\n", *warmup)
	wconn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect for warmup: %v\n", err)
		os.Exit(1)
	}
	for i := 0; i < *warmup; i++ {
		key := []byte(fmt.Sprintf("key:%d", i))
		if err := sendCommand(wconn, protocol.CmdSet, key, value); err != nil {
			fmt.Fprintf(os.Stderr, "warmup send error: %v\n", err)
			os.Exit(1)
		}
		if _, _, err := readResponse(wconn); err != nil {
			fmt.Fprintf(os.Stderr, "warmup read error: %v\n", err)
			os.Exit(1)
		}
	}
	wconn.Close()
	fmt.Println("Warmup complete")

	// Benchmark phase
	requestsPerClient := *requests / *clients
	results := make(chan []time.Duration, *clients)

	var wg sync.WaitGroup
	start := time.Now()

	for c := 0; c < *clients; c++ {
		wg.Add(1)
		c := c
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "client %d: connect error: %v\n", c, err)
				results <- nil
				return
			}
			defer conn.Close()

			startIdx := c * requestsPerClient
			latencies := make([]time.Duration, 0, requestsPerClient)

			for i := 0; i < requestsPerClient; i++ {
				reqIdx := startIdx + i
				key := []byte(fmt.Sprintf("key:%d", reqIdx))

				var cmdType byte
				switch *operation {
				case "set":
					cmdType = protocol.CmdSet
				case "get":
					cmdType = protocol.CmdGet
				default: // "mixed"
					if reqIdx%2 == 0 {
						cmdType = protocol.CmdSet
					} else {
						cmdType = protocol.CmdGet
					}
				}

				t := time.Now()
				var sendVal []byte
				if cmdType == protocol.CmdSet {
					sendVal = value
				}
				if err := sendCommand(conn, cmdType, key, sendVal); err != nil {
					break
				}
				if _, _, err := readResponse(conn); err != nil {
					break
				}
				latencies = append(latencies, time.Since(t))
			}

			results <- latencies
		}()
	}

	wg.Wait()
	total := time.Since(start)
	close(results)

	// Collect all latencies
	var all []time.Duration
	for lats := range results {
		all = append(all, lats...)
	}

	if len(all) == 0 {
		fmt.Fprintln(os.Stderr, "no results collected")
		os.Exit(1)
	}

	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	n := len(all)

	pct := func(p float64) time.Duration {
		idx := int(float64(n) * p)
		if idx >= n {
			idx = n - 1
		}
		return all[idx]
	}
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

	fmt.Printf("\n============ BENCHMARK RESULTS ============\n")
	fmt.Printf("Clients:          %d\n", *clients)
	fmt.Printf("Total requests:   %d\n", n)
	fmt.Printf("Total duration:   %.2fs\n", total.Seconds())
	fmt.Printf("Throughput:       %.0f ops/sec\n", float64(n)/total.Seconds())
	fmt.Printf("\nLatency percentiles:\n")
	fmt.Printf("p50:    %.2fms\n", ms(pct(0.50)))
	fmt.Printf("p75:    %.2fms\n", ms(pct(0.75)))
	fmt.Printf("p90:    %.2fms\n", ms(pct(0.90)))
	fmt.Printf("p95:    %.2fms\n", ms(pct(0.95)))
	fmt.Printf("p99:    %.2fms\n", ms(pct(0.99)))
	fmt.Printf("p999:   %.2fms\n", ms(pct(0.999)))
	fmt.Printf("max:    %.2fms\n", ms(all[n-1]))
	fmt.Printf("===========================================\n")
}

func sendCommand(conn net.Conn, cmdType byte, key, value []byte) error {
	if _, err := conn.Write([]byte{cmdType}); err != nil {
		return err
	}
	if err := binary.Write(conn, binary.BigEndian, uint32(len(key))); err != nil {
		return err
	}
	if len(key) > 0 {
		if _, err := conn.Write(key); err != nil {
			return err
		}
	}
	if err := binary.Write(conn, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	if len(value) > 0 {
		if _, err := conn.Write(value); err != nil {
			return err
		}
	}
	return nil
}

func readResponse(conn net.Conn) (byte, []byte, error) {
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, statusBuf); err != nil {
		return 0, nil, err
	}
	var length uint32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(conn, payload); err != nil {
			return 0, nil, err
		}
	}
	return statusBuf[0], payload, nil
}
