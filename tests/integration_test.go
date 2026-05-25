package integration_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/leahmarymathew/kv-store/internal/cluster"
	"github.com/leahmarymathew/kv-store/internal/protocol"
	"github.com/leahmarymathew/kv-store/internal/replication"
	"github.com/leahmarymathew/kv-store/internal/server"
	"github.com/leahmarymathew/kv-store/internal/store"
	"github.com/leahmarymathew/kv-store/internal/wal"
	"github.com/stretchr/testify/require"
)

// --- test helpers ---

func newWALStore(t *testing.T) *store.WALStore {
	t.Helper()
	ws, err := store.NewWALStoreWithRecovery(filepath.Join(t.TempDir(), "test.wal"))
	require.NoError(t, err)
	t.Cleanup(func() { ws.Close() })
	return ws
}

func newWALStoreAt(t *testing.T, path string) *store.WALStore {
	t.Helper()
	ws, err := store.NewWALStoreWithRecovery(path)
	require.NoError(t, err)
	t.Cleanup(func() { ws.Close() })
	return ws
}

// startPrimaryServer creates a Primary on a random replication port and a
// Server on a random client port. Returns both plus their addresses.
func startPrimaryServer(t *testing.T, ws *store.WALStore) (*replication.Primary, *server.Server, string) {
	t.Helper()
	p := replication.NewPrimary(ws, 0) // port 0 → OS assigns
	require.NoError(t, p.Start())
	t.Cleanup(p.Stop)

	backend := server.NewPrimaryBackend(p)
	srv := server.NewServer(server.Config{}, backend)
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)

	return p, srv, srv.Addr()
}

// startReplicaServer creates a Replica syncing from replAddr and a read-only
// Server on a random client port. Returns the server address.
func startReplicaServer(t *testing.T, ws *store.WALStore, replAddr, id string) (*server.Server, string) {
	t.Helper()
	rep := replication.NewReplica(ws, replAddr, id)
	require.NoError(t, rep.StartSync())

	srv := server.NewServer(server.Config{ReadOnly: true}, ws)
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)

	return srv, srv.Addr()
}

// tcpSet sends a SET command and asserts OK response.
func tcpSet(t *testing.T, conn net.Conn, key, value string) {
	t.Helper()
	require.NoError(t, sendCmd(conn, protocol.CmdSet, []byte(key), []byte(value)))
	status, _, err := readResp(conn)
	require.NoError(t, err)
	require.Equal(t, byte(protocol.StatusOK), status, "SET %q: expected OK", key)
}

// tcpGet sends a GET and returns the value and status byte.
func tcpGet(t *testing.T, conn net.Conn, key string) (byte, string) {
	t.Helper()
	require.NoError(t, sendCmd(conn, protocol.CmdGet, []byte(key), nil))
	status, payload, err := readResp(conn)
	require.NoError(t, err)
	return status, string(payload)
}

func dialConn(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func sendCmd(conn net.Conn, cmdType byte, key, value []byte) error {
	buf := make([]byte, 1+4+len(key)+4+len(value))
	buf[0] = cmdType
	binary.BigEndian.PutUint32(buf[1:], uint32(len(key)))
	copy(buf[5:], key)
	binary.BigEndian.PutUint32(buf[5+len(key):], uint32(len(value)))
	copy(buf[9+len(key):], value)
	_, err := conn.Write(buf)
	return err
}

func readResp(conn net.Conn) (byte, []byte, error) {
	var status [1]byte
	if _, err := io.ReadFull(conn, status[:]); err != nil {
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
	return status[0], payload, nil
}

// --- tests ---

func TestClusterWriteAndReplicate(t *testing.T) {
	primaryWS := newWALStore(t)
	primary, _, primaryAddr := startPrimaryServer(t, primaryWS)

	rep1WS := newWALStore(t)
	_, rep1Addr := startReplicaServer(t, rep1WS, primary.Addr(), "rep1")

	rep2WS := newWALStore(t)
	_, rep2Addr := startReplicaServer(t, rep2WS, primary.Addr(), "rep2")

	time.Sleep(50 * time.Millisecond) // let replicas finish connecting

	// Write 500 keys to primary
	pConn := dialConn(t, primaryAddr)
	for i := 0; i < 500; i++ {
		tcpSet(t, pConn, fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i))
	}

	time.Sleep(500 * time.Millisecond) // async replication window

	// Verify all keys on rep1
	r1Conn := dialConn(t, rep1Addr)
	for i := 0; i < 500; i++ {
		status, val := tcpGet(t, r1Conn, fmt.Sprintf("key-%d", i))
		require.Equal(t, byte(protocol.StatusOK), status, "rep1 missing key-%d", i)
		require.Equal(t, fmt.Sprintf("val-%d", i), val, "rep1 wrong value for key-%d", i)
	}

	// Verify all keys on rep2
	r2Conn := dialConn(t, rep2Addr)
	for i := 0; i < 500; i++ {
		status, val := tcpGet(t, r2Conn, fmt.Sprintf("key-%d", i))
		require.Equal(t, byte(protocol.StatusOK), status, "rep2 missing key-%d", i)
		require.Equal(t, fmt.Sprintf("val-%d", i), val, "rep2 wrong value for key-%d", i)
	}
}

func TestClusterReadOnly(t *testing.T) {
	primaryWS := newWALStore(t)
	primary, _, _ := startPrimaryServer(t, primaryWS)

	repWS := newWALStore(t)
	_, repAddr := startReplicaServer(t, repWS, primary.Addr(), "rep-ro")
	time.Sleep(30 * time.Millisecond)

	conn := dialConn(t, repAddr)
	require.NoError(t, sendCmd(conn, protocol.CmdSet, []byte("k"), []byte("v")))
	status, payload, err := readResp(conn)
	require.NoError(t, err)
	require.Equal(t, byte(protocol.StatusError), status, "expected error status from replica SET")
	require.Contains(t, string(payload), "write refused", "error should mention 'write refused'")
}

func TestClusterConsistentHashing(t *testing.T) {
	router := cluster.NewRouter(150)
	router.RegisterNode(cluster.NodeAddress{ID: "node1", Host: "localhost", Port: 7379})
	router.RegisterNode(cluster.NodeAddress{ID: "node2", Host: "localhost", Port: 7381})
	router.RegisterNode(cluster.NodeAddress{ID: "node3", Host: "localhost", Port: 7382})

	total := 10000
	counts := map[cluster.NodeID]int{}
	for i := 0; i < total; i++ {
		addr, ok := router.RouteKey(fmt.Sprintf("key-%d", i))
		require.True(t, ok)
		counts[addr.ID]++
	}

	for id, count := range counts {
		pct := float64(count) / float64(total) * 100
		t.Logf("node %s: %d keys (%.1f%%)", id, count, pct)
		require.GreaterOrEqual(t, pct, 20.0, "node %s below 20%%", id)
		require.LessOrEqual(t, pct, 40.0, "node %s above 40%%", id)
	}

	// Same key always routes to same node
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		a1, _ := router.RouteKey(key)
		a2, _ := router.RouteKey(key)
		require.Equal(t, a1.ID, a2.ID, "routing inconsistent for key %q", key)
	}
}

func TestFullPipelineWithWAL(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "primary.wal")

	// Phase 1: start primary, write 200 keys
	primaryWS := newWALStoreAt(t, walPath)
	primary, primarySrv, primaryAddr := startPrimaryServer(t, primaryWS)
	time.Sleep(20 * time.Millisecond)

	conn := dialConn(t, primaryAddr)
	for i := 0; i < 200; i++ {
		tcpSet(t, conn, fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i))
	}
	conn.Close()

	// Phase 2: stop primary server (simulates crash/restart)
	replAddr := primary.Addr()
	primarySrv.Stop()
	primary.Stop()
	// WALStore cleanup is registered via t.Cleanup; it will close the file.
	// We open a second WALStore on the same path to simulate recovery.

	// Phase 3: recover primary from WAL, verify 200 keys present
	w, err := wal.NewWAL(walPath)
	require.NoError(t, err)
	recoveredStore := store.NewStore()
	wal.Recover(w, recoveredStore)
	w.Close()

	for i := 0; i < 200; i++ {
		_, ok := recoveredStore.Get(fmt.Sprintf("key-%d", i))
		require.True(t, ok, "recovered store missing key-%d", i)
	}

	// Phase 4: restart primary with the recovered WAL, start replicas
	recoveredWS, err := store.NewWALStoreWithRecovery(walPath)
	require.NoError(t, err)
	t.Cleanup(func() { recoveredWS.Close() })

	p2 := replication.NewPrimary(recoveredWS, 0)
	require.NoError(t, p2.Start())
	t.Cleanup(p2.Stop)
	newReplAddr := p2.Addr()
	_ = replAddr // original addr no longer in use

	backend2 := server.NewPrimaryBackend(p2)
	srv2 := server.NewServer(server.Config{}, backend2)
	require.NoError(t, srv2.Start())
	t.Cleanup(srv2.Stop)

	repWS := newWALStore(t)
	rep := replication.NewReplica(repWS, newReplAddr, "rep-recovery")
	require.NoError(t, rep.StartSync())

	time.Sleep(300 * time.Millisecond) // give replica time to receive full-sync

	for i := 0; i < 200; i++ {
		_, ok := repWS.Get(fmt.Sprintf("key-%d", i))
		require.True(t, ok, "replica missing key-%d after WAL recovery sync", i)
	}
}
