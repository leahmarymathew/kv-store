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
	"strings"
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
	clusterAddrs := flag.String("cluster-addrs", "", "comma-separated addresses for cluster benchmark e.g. localhost:7379,localhost:7381,localhost:7382")
	flag.Parse()

	if *clusterAddrs != "" {
		addrs := strings.Split(*clusterAddrs, ",")
		runClusterBenchmark(addrs, *clients, *requests, *payloadSize)
		return
	}

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

func runClusterBenchmark(addrs []string, numClients, numRequests, payloadSize int) {
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "no cluster addresses provided")
		os.Exit(1)
	}
	primaryAddr := addrs[0]
	replicaAddrs := addrs[1:]
	if len(replicaAddrs) == 0 {
		replicaAddrs = addrs // if only one addr, read from it too
	}
	value := bytes.Repeat([]byte("x"), payloadSize)
	perClient := numRequests / numClients

	// --- Write benchmark (all writes to primary) ---
	writeResults := make(chan []time.Duration, numClients)
	var wg sync.WaitGroup
	writeStart := time.Now()

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		c := c
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", primaryAddr)
			if err != nil {
				writeResults <- nil
				return
			}
			defer conn.Close()

			lats := make([]time.Duration, 0, perClient)
			for i := 0; i < perClient; i++ {
				key := []byte(fmt.Sprintf("wkey:%d:%d", c, i))
				t := time.Now()
				if err := sendCommand(conn, protocol.CmdSet, key, value); err != nil {
					break
				}
				if _, _, err := readResponse(conn); err != nil {
					break
				}
				lats = append(lats, time.Since(t))
			}
			writeResults <- lats
		}()
	}
	wg.Wait()
	writeDuration := time.Since(writeStart)
	close(writeResults)

	var writeAll []time.Duration
	for l := range writeResults {
		writeAll = append(writeAll, l...)
	}

	// --- Read benchmark (round-robin across all addresses) ---
	readResults := make(chan []time.Duration, numClients)
	readStart := time.Now()

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		c := c
		go func() {
			defer wg.Done()
			addr := replicaAddrs[c%len(replicaAddrs)]
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				readResults <- nil
				return
			}
			defer conn.Close()

			lats := make([]time.Duration, 0, perClient)
			for i := 0; i < perClient; i++ {
				key := []byte(fmt.Sprintf("wkey:%d:%d", c, i))
				t := time.Now()
				if err := sendCommand(conn, protocol.CmdGet, key, nil); err != nil {
					break
				}
				if _, _, err := readResponse(conn); err != nil {
					break
				}
				lats = append(lats, time.Since(t))
			}
			readResults <- lats
		}()
	}
	wg.Wait()
	readDuration := time.Since(readStart)
	close(readResults)

	var readAll []time.Duration
	for l := range readResults {
		readAll = append(readAll, l...)
	}

	// --- Replication lag measurement (50 samples) ---
	lagSamples := 50
	var lagTotals []time.Duration

	lagPrimary, err := net.Dial("tcp", primaryAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lag: connect to primary failed: %v\n", err)
	} else {
		defer lagPrimary.Close()
		for i := 0; i < lagSamples; i++ {
			replicaAddr := replicaAddrs[i%len(replicaAddrs)]
			lagReplica, err := net.Dial("tcp", replicaAddr)
			if err != nil {
				continue
			}

			key := []byte(fmt.Sprintf("lagkey:%d", i))
			writeTime := time.Now()
			sendCommand(lagPrimary, protocol.CmdSet, key, value)
			readResponse(lagPrimary)

			// Poll replica until the key appears or 500ms passes.
			var lag time.Duration
			deadline := time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(deadline) {
				sendCommand(lagReplica, protocol.CmdGet, key, nil)
				status, _, _ := readResponse(lagReplica)
				if status == protocol.StatusOK {
					lag = time.Since(writeTime)
					break
				}
				time.Sleep(time.Millisecond)
			}
			if lag == 0 {
				lag = 500 * time.Millisecond // timed out
			}
			lagTotals = append(lagTotals, lag)
			lagReplica.Close()
		}
	}

	// --- Output ---
	writeThroughput := float64(len(writeAll)) / writeDuration.Seconds()
	readThroughput := float64(len(readAll)) / readDuration.Seconds()

	var avgLag, maxLag time.Duration
	if len(lagTotals) > 0 {
		var sum time.Duration
		for _, l := range lagTotals {
			sum += l
			if l > maxLag {
				maxLag = l
			}
		}
		avgLag = sum / time.Duration(len(lagTotals))
	}

	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

	fmt.Printf("\n============ CLUSTER BENCHMARK ============\n")
	fmt.Printf("Primary:           %s\n", primaryAddr)
	fmt.Printf("Replicas:          %s\n", strings.Join(replicaAddrs, ", "))
	fmt.Printf("Primary writes:    %.0f ops/sec (%d ops)\n", writeThroughput, len(writeAll))
	fmt.Printf("Distributed reads: %.0f ops/sec (%d ops)\n", readThroughput, len(readAll))
	fmt.Printf("Avg replication lag: %.2fms\n", ms(avgLag))
	fmt.Printf("Max replication lag: %.2fms\n", ms(maxLag))
	fmt.Printf("==========================================\n")
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
