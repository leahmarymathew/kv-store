package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leahmarymathew/kv-store/internal/protocol"
	"github.com/leahmarymathew/kv-store/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestWALStore creates a WALStore backed by a temp-dir WAL file and
// registers ws.Close() as a cleanup so the file handle is released before
// t.TempDir() tries to remove the directory (required on Windows).
func newTestWALStore(t *testing.T) *store.WALStore {
	t.Helper()
	ws, err := store.NewWALStoreWithRecovery(filepath.Join(t.TempDir(), "test.wal"))
	require.NoError(t, err)
	t.Cleanup(func() { ws.Close() })
	return ws
}

// startTestServer creates a WALStore and server on a random OS-assigned port,
// registers Stop via t.Cleanup, and returns both.
func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	srv := NewServer(Config{}, newTestWALStore(t))
	require.NoError(t, srv.Start())
	addr := srv.listener.Addr().String()
	t.Cleanup(srv.Stop)
	return srv, addr
}

// dialAndPing dials addr, sends PING, verifies PONG, and returns the open conn.
func dialAndPing(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	sendCommand(t, conn, protocol.CmdPing, nil, nil)
	status, _ := mustReadResponse(t, conn)
	require.Equal(t, byte(protocol.StatusPong), status, "expected PONG from server")
	return conn
}

// sendCommand encodes and writes a command frame to conn.
// Must only be called from the test goroutine (uses require).
func sendCommand(t *testing.T, conn net.Conn, cmdType byte, key, value []byte) {
	t.Helper()
	_, err := conn.Write([]byte{cmdType})
	require.NoError(t, err)
	require.NoError(t, binary.Write(conn, binary.BigEndian, uint32(len(key))))
	if len(key) > 0 {
		_, err = conn.Write(key)
		require.NoError(t, err)
	}
	require.NoError(t, binary.Write(conn, binary.BigEndian, uint32(len(value))))
	if len(value) > 0 {
		_, err = conn.Write(value)
		require.NoError(t, err)
	}
}

// mustReadResponse reads [status][4-byte len][payload] from conn.
// Must only be called from the test goroutine (uses require).
func mustReadResponse(t *testing.T, conn net.Conn) (byte, []byte) {
	t.Helper()
	statusBuf := make([]byte, 1)
	_, err := io.ReadFull(conn, statusBuf)
	require.NoError(t, err)
	var length uint32
	require.NoError(t, binary.Read(conn, binary.BigEndian, &length))
	payload := make([]byte, length)
	if length > 0 {
		_, err = io.ReadFull(conn, payload)
		require.NoError(t, err)
	}
	return statusBuf[0], payload
}

// buildCommandFrame encodes a command to raw bytes without using testing.T.
// Safe to call from goroutines.
func buildCommandFrame(cmdType byte, key, value []byte) []byte {
	buf := make([]byte, 1+4+len(key)+4+len(value))
	buf[0] = cmdType
	binary.BigEndian.PutUint32(buf[1:], uint32(len(key)))
	copy(buf[5:], key)
	binary.BigEndian.PutUint32(buf[5+len(key):], uint32(len(value)))
	copy(buf[9+len(key):], value)
	return buf
}

// readResponseRaw reads a response without using testing.T.
// Safe to call from goroutines.
func readResponseRaw(conn net.Conn) (byte, []byte, error) {
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

// --- Tests ---

func TestServerStartStop(t *testing.T) {
	t.Parallel()
	srv := NewServer(Config{}, newTestWALStore(t))
	require.NoError(t, srv.Start())
	addr := srv.listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err, "server should be reachable after Start")
	conn.Close()

	srv.Stop()

	_, err = net.DialTimeout("tcp", addr, 200*time.Millisecond)
	assert.Error(t, err, "server should not be reachable after Stop")
}

func TestPingCommand(t *testing.T) {
	t.Parallel()
	_, addr := startTestServer(t)
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	sendCommand(t, conn, protocol.CmdPing, nil, nil)
	status, _ := mustReadResponse(t, conn)
	assert.Equal(t, byte(protocol.StatusPong), status)
}

func TestSetGet(t *testing.T) {
	t.Parallel()
	_, addr := startTestServer(t)
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	sendCommand(t, conn, protocol.CmdSet, []byte("foo"), []byte("bar"))
	status, _ := mustReadResponse(t, conn)
	require.Equal(t, byte(protocol.StatusOK), status)

	sendCommand(t, conn, protocol.CmdGet, []byte("foo"), nil)
	status, payload := mustReadResponse(t, conn)
	assert.Equal(t, byte(protocol.StatusOK), status)
	assert.Equal(t, []byte("bar"), payload)
}

func TestGetMissing(t *testing.T) {
	t.Parallel()
	_, addr := startTestServer(t)
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	sendCommand(t, conn, protocol.CmdGet, []byte("nonexistent"), nil)
	status, _ := mustReadResponse(t, conn)
	assert.Equal(t, byte(protocol.StatusNotFound), status)
}

func TestDeleteFlow(t *testing.T) {
	t.Parallel()
	_, addr := startTestServer(t)
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	sendCommand(t, conn, protocol.CmdSet, []byte("key"), []byte("value"))
	status, _ := mustReadResponse(t, conn)
	require.Equal(t, byte(protocol.StatusOK), status)

	sendCommand(t, conn, protocol.CmdDelete, []byte("key"), nil)
	status, _ = mustReadResponse(t, conn)
	require.Equal(t, byte(protocol.StatusOK), status)

	sendCommand(t, conn, protocol.CmdGet, []byte("key"), nil)
	status, _ = mustReadResponse(t, conn)
	assert.Equal(t, byte(protocol.StatusNotFound), status)
}

func TestConcurrentClients(t *testing.T) {
	t.Parallel()
	ws := newTestWALStore(t)
	srv := NewServer(Config{MaxConns: 200}, ws)
	require.NoError(t, srv.Start())
	addr := srv.listener.Addr().String()
	defer srv.Stop()

	var wg sync.WaitGroup
	var errCount int64

	for i := 0; i < 100; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				atomic.AddInt64(&errCount, 1)
				return
			}
			defer conn.Close()

			for j := 0; j < 10; j++ {
				key := []byte(fmt.Sprintf("goroutine%d-key%d", i, j))
				val := []byte(fmt.Sprintf("value%d-%d", i, j))
				if _, err := conn.Write(buildCommandFrame(protocol.CmdSet, key, val)); err != nil {
					atomic.AddInt64(&errCount, 1)
					return
				}
				status, _, err := readResponseRaw(conn)
				if err != nil || status != protocol.StatusOK {
					atomic.AddInt64(&errCount, 1)
					return
				}
			}

			for j := 0; j < 10; j++ {
				key := []byte(fmt.Sprintf("goroutine%d-key%d", i, j))
				expected := fmt.Sprintf("value%d-%d", i, j)
				if _, err := conn.Write(buildCommandFrame(protocol.CmdGet, key, nil)); err != nil {
					atomic.AddInt64(&errCount, 1)
					return
				}
				status, payload, err := readResponseRaw(conn)
				if err != nil || status != protocol.StatusOK || string(payload) != expected {
					atomic.AddInt64(&errCount, 1)
					return
				}
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(0), atomic.LoadInt64(&errCount), "expected no errors across concurrent clients")
	assert.Equal(t, 1000, ws.Len(), "expected 1000 keys in store")
}

func TestConnectionLimit(t *testing.T) {
	t.Parallel()
	srv := NewServer(Config{MaxConns: 5}, newTestWALStore(t))
	require.NoError(t, srv.Start())
	addr := srv.listener.Addr().String()
	defer srv.Stop()

	conns := make([]net.Conn, 5)
	for i := range conns {
		conns[i] = dialAndPing(t, addr)
	}
	defer func() {
		for _, c := range conns {
			if c != nil {
				c.Close()
			}
		}
	}()

	assert.Eventually(t, func() bool {
		return srv.ActiveConnections() == 5
	}, time.Second, 10*time.Millisecond, "expected 5 active connections")

	// 6th connection: TCP handshake may succeed but server closes it immediately.
	conn6, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn6.Close()

	conn6.SetDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = conn6.Read(buf)
	assert.Error(t, err, "6th connection should be closed by server at capacity")

	// Close one connection and verify a new one is accepted.
	conns[0].Close()
	conns[0] = nil

	assert.Eventually(t, func() bool {
		return srv.ActiveConnections() < 5
	}, time.Second, 10*time.Millisecond, "semaphore slot should free up after close")

	newConn := dialAndPing(t, addr)
	defer newConn.Close()
}

func TestServerHandlesClientDisconnect(t *testing.T) {
	t.Parallel()
	_, addr := startTestServer(t)

	// Connect and immediately close without sending any data.
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	conn.Close()

	// Give the server goroutine time to observe the EOF and exit.
	time.Sleep(50 * time.Millisecond)

	// Server must still accept new connections.
	newConn := dialAndPing(t, addr)
	defer newConn.Close()
}
