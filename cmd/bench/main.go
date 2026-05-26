package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leahmarymathew/kv-store/internal/protocol"
)

var benchValue []byte

// ─── types ────────────────────────────────────────────────────────────────────

type throughputRow struct {
	clients             int
	ops                 int
	throughput          float64
	p50, p95, p99, p999 time.Duration
}

type stressRow struct {
	clients    int
	throughput float64
	p99        time.Duration
	errorRate  float64
}

type pipelineRow struct {
	size       int
	throughput float64
	ratio      float64
}

var latencyBuckets = []struct {
	label      string
	min, max   time.Duration
}{
	{"< 0.1ms", 0, 100 * time.Microsecond},
	{"0.1-0.5ms", 100 * time.Microsecond, 500 * time.Microsecond},
	{"0.5-1ms", 500 * time.Microsecond, time.Millisecond},
	{"1-5ms", time.Millisecond, 5 * time.Millisecond},
	{"5-10ms", 5 * time.Millisecond, 10 * time.Millisecond},
	{"> 10ms", 10 * time.Millisecond, 1<<63 - 1},
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	host := flag.String("host", "localhost", "server host")
	port := flag.Int("port", 7379, "server port")
	clients := flag.Int("clients", 50, "concurrent clients (used when mode=throughput with custom run)")
	requests := flag.Int("requests", 10000, "total requests (used when mode=throughput with custom run)")
	payloadSize := flag.Int("payload-size", 64, "value size in bytes")
	operation := flag.String("operation", "mixed", "operation type: get, set, or mixed")
	warmup := flag.Int("warmup", 1000, "warmup requests before measuring")
	mode := flag.String("mode", "throughput", "benchmark mode: throughput, latency, stress, pipeline")
	output := flag.String("output", "text", "output format: text or csv")
	clusterAddrs := flag.String("cluster-addrs", "", "comma-separated addresses for cluster benchmark")
	flag.Parse()

	benchValue = bytes.Repeat([]byte("x"), *payloadSize)

	if *clusterAddrs != "" {
		addrs := strings.Split(*clusterAddrs, ",")
		runClusterBenchmark(addrs, *clients, *requests, *payloadSize)
		return
	}

	addr := net.JoinHostPort(*host, strconv.Itoa(*port))
	doWarmup(addr, *warmup)

	switch *mode {
	case "latency":
		runLatencyMode(addr, *output)
	case "stress":
		runStressMode(addr, *output)
	case "pipeline":
		runPipelineMode(addr, *output)
	default: // throughput
		runThroughputMode(addr, *clients, *requests, *operation, *output)
	}
}

// ─── warmup ───────────────────────────────────────────────────────────────────

func doWarmup(addr string, n int) {
	if n == 0 {
		return
	}
	fmt.Printf("Warming up with %d requests...\n", n)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warmup connect failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("warmup:%d", i))
		sendCommand(conn, protocol.CmdSet, key, benchValue)
		readResponse(conn)
	}
	fmt.Println("Warmup complete.")
}

// ─── throughput mode ──────────────────────────────────────────────────────────

func runThroughputMode(addr string, defaultClients, defaultRequests int, operation, outputFmt string) {
	levels := []int{50, 100, 200, 500}
	var rows []throughputRow

	fmt.Printf("\n============ THROUGHPUT BENCHMARK ============\n")
	fmt.Printf("%-10s %-14s %-9s %-9s %-9s %-9s\n", "Clients", "Ops/sec", "p50", "p95", "p99", "p999")
	fmt.Printf("%-10s %-14s %-9s %-9s %-9s %-9s\n", "-------", "-------", "---", "---", "---", "----")

	for _, nc := range levels {
		reqs := nc * 200
		lats, dur := runBatch(addr, nc, reqs, operation)
		if len(lats) == 0 {
			continue
		}
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		n := len(lats)
		r := throughputRow{
			clients:    nc,
			ops:        n,
			throughput: float64(n) / dur.Seconds(),
			p50:        pctile(lats, 0.50),
			p95:        pctile(lats, 0.95),
			p99:        pctile(lats, 0.99),
			p999:       pctile(lats, 0.999),
		}
		rows = append(rows, r)
		fmt.Printf("%-10d %-14.0f %-9s %-9s %-9s %-9s\n",
			nc, r.throughput, fmtMs(r.p50), fmtMs(r.p95), fmtMs(r.p99), fmtMs(r.p999))
	}
	fmt.Printf("==============================================\n")

	if outputFmt == "csv" {
		writeThroughputCSV(rows)
	}
}

// ─── latency mode ─────────────────────────────────────────────────────────────

func runLatencyMode(addr, outputFmt string) {
	const numClients = 5
	const numRequests = 100_000

	fmt.Printf("Running latency mode: %d clients, %d requests...\n", numClients, numRequests)
	lats, _ := runBatch(addr, numClients, numRequests, "mixed")
	if len(lats) == 0 {
		fmt.Fprintln(os.Stderr, "no results")
		return
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	n := len(lats)

	fmt.Printf("\n============ LATENCY DISTRIBUTION ============\n")
	fmt.Printf("Total requests: %d\n", n)
	fmt.Printf("min:   %s\n", fmtMicro(lats[0]))
	fmt.Printf("p50:   %s\n", fmtMicro(pctile(lats, 0.50)))
	fmt.Printf("p75:   %s\n", fmtMicro(pctile(lats, 0.75)))
	fmt.Printf("p90:   %s\n", fmtMicro(pctile(lats, 0.90)))
	fmt.Printf("p95:   %s\n", fmtMicro(pctile(lats, 0.95)))
	fmt.Printf("p99:   %s\n", fmtMicro(pctile(lats, 0.99)))
	fmt.Printf("p999:  %s\n", fmtMicro(pctile(lats, 0.999)))
	fmt.Printf("max:   %s\n", fmtMicro(lats[n-1]))

	counts := make([]int, len(latencyBuckets))
	for _, l := range lats {
		for i, b := range latencyBuckets {
			if l >= b.min && l < b.max {
				counts[i]++
				break
			}
		}
	}

	fmt.Printf("\n%-13s %-10s %s\n", "Bucket", "Count", "Percentage")
	fmt.Printf("%-13s %-10s %s\n", "------", "-----", "----------")
	for i, b := range latencyBuckets {
		fmt.Printf("%-13s %-10d %.1f%%\n", b.label, counts[i], float64(counts[i])/float64(n)*100)
	}
	fmt.Printf("==============================================\n")

	if outputFmt == "csv" {
		writeLatencyCSV(lats, counts, n)
	}
}

// ─── stress mode ──────────────────────────────────────────────────────────────

func runStressMode(addr, outputFmt string) {
	fmt.Printf("\n============ STRESS TEST ============\n")
	fmt.Printf("%-10s %-14s %-12s %-10s\n", "Clients", "Ops/sec", "p99", "Error%")
	fmt.Printf("%-10s %-14s %-12s %-10s\n", "-------", "-------", "---", "------")

	var rows []stressRow
	for nc := 10; nc <= 500; nc += 10 {
		lats, errs, dur := runTimedBatch(addr, nc, 5*time.Second)
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

		total := len(lats) + errs
		errorRate := 0.0
		if total > 0 {
			errorRate = float64(errs) / float64(total) * 100
		}

		var p99 time.Duration
		throughput := 0.0
		if len(lats) > 0 {
			p99 = pctile(lats, 0.99)
			throughput = float64(len(lats)) / dur.Seconds()
		}

		r := stressRow{nc, throughput, p99, errorRate}
		rows = append(rows, r)
		fmt.Printf("%-10d %-14.0f %-12s %.2f%%\n", nc, throughput, fmtMs(p99), errorRate)

		if errorRate > 1.0 {
			fmt.Printf("  stopping: error rate %.2f%% exceeded 1%%\n", errorRate)
			break
		}
		if p99 > 100*time.Millisecond {
			fmt.Printf("  stopping: p99 %s exceeded 100ms\n", fmtMs(p99))
			break
		}
	}
	fmt.Printf("=====================================\n")

	if outputFmt == "csv" {
		writeStressCSV(rows)
	}
}

// runTimedBatch runs numClients goroutines for duration, returning latencies, errors, actual elapsed.
func runTimedBatch(addr string, numClients int, duration time.Duration) ([]time.Duration, int, time.Duration) {
	deadline := time.Now().Add(duration)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errCount int64
	allLats := make([][]time.Duration, numClients)

	start := time.Now()
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		c := c
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}
			defer conn.Close()

			var myLats []time.Duration
			i := 0
			for time.Now().Before(deadline) {
				key := []byte(fmt.Sprintf("stress:%d:%d", c, i))
				var cmdType byte = protocol.CmdSet
				var val []byte
				if i%2 == 0 {
					val = benchValue
				} else {
					cmdType = protocol.CmdGet
				}
				t := time.Now()
				if err := sendCommand(conn, cmdType, key, val); err != nil {
					atomic.AddInt64(&errCount, 1)
					break
				}
				status, _, err := readResponse(conn)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					break
				}
				if status != protocol.StatusOK && status != protocol.StatusNotFound {
					atomic.AddInt64(&errCount, 1)
				} else {
					myLats = append(myLats, time.Since(t))
				}
				i++
			}

			mu.Lock()
			allLats[c] = myLats
			mu.Unlock()
		}()
	}
	wg.Wait()
	actual := time.Since(start)

	var combined []time.Duration
	for _, l := range allLats {
		combined = append(combined, l...)
	}
	return combined, int(errCount), actual
}

// ─── pipeline mode ────────────────────────────────────────────────────────────

func runPipelineMode(addr, outputFmt string) {
	sizes := []int{1, 4, 8, 16}

	fmt.Printf("\n============ PIPELINE BENCHMARK ============\n")
	fmt.Printf("%-14s %-16s %-10s\n", "Pipeline size", "Throughput", "vs serial")
	fmt.Printf("%-14s %-16s %-10s\n", "-------------", "----------", "---------")

	var rows []pipelineRow
	var baselineThroughput float64

	for _, psize := range sizes {
		lats, dur := runPipelineBatch(addr, 10, 10_000, psize)
		n := len(lats)
		throughput := float64(n) / dur.Seconds()

		ratio := 1.0
		if baselineThroughput == 0 {
			baselineThroughput = throughput
		} else {
			ratio = throughput / baselineThroughput
		}

		rows = append(rows, pipelineRow{psize, throughput, ratio})
		fmt.Printf("%-14d %-16.0f %.2fx\n", psize, throughput, ratio)
	}
	fmt.Printf("============================================\n")

	if outputFmt == "csv" {
		writePipelineCSV(rows)
	}
}

func runPipelineBatch(addr string, numClients, totalRequests, pipelineSize int) ([]time.Duration, time.Duration) {
	perClient := totalRequests / numClients
	results := make(chan []time.Duration, numClients)
	var wg sync.WaitGroup
	start := time.Now()

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		c := c
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				results <- nil
				return
			}
			defer conn.Close()

			bw := bufio.NewWriterSize(conn, 64*1024)
			var lats []time.Duration
			batches := perClient / pipelineSize

			for b := 0; b < batches; b++ {
				t := time.Now()
				for i := 0; i < pipelineSize; i++ {
					idx := c*perClient + b*pipelineSize + i
					key := []byte(fmt.Sprintf("pipe:%d", idx))
					writeCmd(bw, protocol.CmdSet, key, benchValue)
				}
				if err := bw.Flush(); err != nil {
					break
				}
				ok := true
				for i := 0; i < pipelineSize; i++ {
					if _, _, err := readResponse(conn); err != nil {
						ok = false
						break
					}
				}
				if !ok {
					break
				}
				perCmd := time.Since(t) / time.Duration(pipelineSize)
				for i := 0; i < pipelineSize; i++ {
					lats = append(lats, perCmd)
				}
			}
			results <- lats
		}()
	}

	wg.Wait()
	dur := time.Since(start)
	close(results)

	var all []time.Duration
	for l := range results {
		all = append(all, l...)
	}
	return all, dur
}

// ─── core batch runner ────────────────────────────────────────────────────────

func runBatch(addr string, numClients, numRequests int, operation string) ([]time.Duration, time.Duration) {
	perClient := numRequests / numClients
	results := make(chan []time.Duration, numClients)
	var wg sync.WaitGroup
	start := time.Now()

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		c := c
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				results <- nil
				return
			}
			defer conn.Close()

			startIdx := c * perClient
			lats := make([]time.Duration, 0, perClient)

			for i := 0; i < perClient; i++ {
				idx := startIdx + i
				key := []byte(fmt.Sprintf("key:%d", idx))
				var cmdType byte
				switch operation {
				case "set":
					cmdType = protocol.CmdSet
				case "get":
					cmdType = protocol.CmdGet
				default:
					if idx%2 == 0 {
						cmdType = protocol.CmdSet
					} else {
						cmdType = protocol.CmdGet
					}
				}
				var val []byte
				if cmdType == protocol.CmdSet {
					val = benchValue
				}
				t := time.Now()
				if err := sendCommand(conn, cmdType, key, val); err != nil {
					break
				}
				if _, _, err := readResponse(conn); err != nil {
					break
				}
				lats = append(lats, time.Since(t))
			}
			results <- lats
		}()
	}

	wg.Wait()
	dur := time.Since(start)
	close(results)

	var all []time.Duration
	for l := range results {
		all = append(all, l...)
	}
	return all, dur
}

// ─── cluster benchmark ────────────────────────────────────────────────────────

func runClusterBenchmark(addrs []string, numClients, numRequests, payloadSize int) {
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "no cluster addresses provided")
		os.Exit(1)
	}
	primaryAddr := addrs[0]
	replicaAddrs := addrs[1:]
	if len(replicaAddrs) == 0 {
		replicaAddrs = addrs
	}
	value := bytes.Repeat([]byte("x"), payloadSize)
	perClient := numRequests / numClients

	// Write benchmark
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
				sendCommand(conn, protocol.CmdSet, key, value)
				readResponse(conn)
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

	// Read benchmark
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
				sendCommand(conn, protocol.CmdGet, key, nil)
				readResponse(conn)
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

	// Replication lag
	lagSamples := 50
	var lagTotals []time.Duration
	lagPrimary, err := net.Dial("tcp", primaryAddr)
	if err == nil {
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
				lag = 500 * time.Millisecond
			}
			lagTotals = append(lagTotals, lag)
			lagReplica.Close()
		}
	}

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

	fmt.Printf("\n============ CLUSTER BENCHMARK ============\n")
	fmt.Printf("Primary:             %s\n", primaryAddr)
	fmt.Printf("Replicas:            %s\n", strings.Join(replicaAddrs, ", "))
	fmt.Printf("Primary writes:      %.0f ops/sec (%d ops)\n", writeThroughput, len(writeAll))
	fmt.Printf("Distributed reads:   %.0f ops/sec (%d ops)\n", readThroughput, len(readAll))
	fmt.Printf("Avg replication lag: %.2fms\n", msF(avgLag))
	fmt.Printf("Max replication lag: %.2fms\n", msF(maxLag))
	fmt.Printf("==========================================\n")
}

// ─── CSV output ───────────────────────────────────────────────────────────────

func csvFile(tag string) (*os.File, *csv.Writer, error) {
	if err := os.MkdirAll("bench_results", 0o755); err != nil {
		return nil, nil, err
	}
	name := fmt.Sprintf("bench_results/%s_%s.csv", tag, time.Now().Format("20060102_150405"))
	f, err := os.Create(name)
	if err != nil {
		return nil, nil, err
	}
	fmt.Printf("CSV written to %s\n", name)
	return f, csv.NewWriter(f), nil
}

func writeThroughputCSV(rows []throughputRow) {
	f, w, err := csvFile("throughput")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer f.Close()
	w.Write([]string{"clients", "ops_per_sec", "p50_ms", "p95_ms", "p99_ms", "p999_ms"})
	for _, r := range rows {
		w.Write([]string{
			strconv.Itoa(r.clients),
			fmt.Sprintf("%.0f", r.throughput),
			fmt.Sprintf("%.3f", msF(r.p50)),
			fmt.Sprintf("%.3f", msF(r.p95)),
			fmt.Sprintf("%.3f", msF(r.p99)),
			fmt.Sprintf("%.3f", msF(r.p999)),
		})
	}
	w.Flush()
}

func writeLatencyCSV(lats []time.Duration, counts []int, n int) {
	f, w, err := csvFile("latency")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer f.Close()
	w.Write([]string{"percentile", "value_ms"})
	for _, p := range []struct {
		label string
		pct   float64
	}{
		{"min", 0}, {"p50", 0.50}, {"p75", 0.75}, {"p90", 0.90},
		{"p95", 0.95}, {"p99", 0.99}, {"p999", 0.999}, {"max", 1.0},
	} {
		var val time.Duration
		switch p.pct {
		case 0:
			val = lats[0]
		case 1.0:
			val = lats[len(lats)-1]
		default:
			val = pctile(lats, p.pct)
		}
		w.Write([]string{p.label, fmt.Sprintf("%.3f", msF(val))})
	}
	w.Write([]string{"", ""})
	w.Write([]string{"bucket", "count", "percentage"})
	for i, b := range latencyBuckets {
		w.Write([]string{b.label, strconv.Itoa(counts[i]), fmt.Sprintf("%.2f", float64(counts[i])/float64(n)*100)})
	}
	w.Flush()
}

func writeStressCSV(rows []stressRow) {
	f, w, err := csvFile("stress")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer f.Close()
	w.Write([]string{"clients", "ops_per_sec", "p99_ms", "error_pct"})
	for _, r := range rows {
		w.Write([]string{
			strconv.Itoa(r.clients),
			fmt.Sprintf("%.0f", r.throughput),
			fmt.Sprintf("%.3f", msF(r.p99)),
			fmt.Sprintf("%.2f", r.errorRate),
		})
	}
	w.Flush()
}

func writePipelineCSV(rows []pipelineRow) {
	f, w, err := csvFile("pipeline")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer f.Close()
	w.Write([]string{"pipeline_size", "ops_per_sec", "ratio_vs_serial"})
	for _, r := range rows {
		w.Write([]string{
			strconv.Itoa(r.size),
			fmt.Sprintf("%.0f", r.throughput),
			fmt.Sprintf("%.2f", r.ratio),
		})
	}
	w.Flush()
}

// ─── protocol helpers ─────────────────────────────────────────────────────────

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

func writeCmd(bw *bufio.Writer, cmdType byte, key, value []byte) {
	var lenBuf [4]byte
	bw.WriteByte(cmdType)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(key)))
	bw.Write(lenBuf[:])
	bw.Write(key)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))
	bw.Write(lenBuf[:])
	bw.Write(value)
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

// ─── formatting helpers ───────────────────────────────────────────────────────

func pctile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	idx := int(float64(n) * p)
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

func fmtMs(d time.Duration) string {
	return fmt.Sprintf("%.2fms", msF(d))
}

func fmtMicro(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	}
	return fmtMs(d)
}

func msF(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
