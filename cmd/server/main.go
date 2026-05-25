package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/leahmarymathew/kv-store/internal/replication"
	"github.com/leahmarymathew/kv-store/internal/server"
	"github.com/leahmarymathew/kv-store/internal/store"
)

func main() {
	host := flag.String("host", "0.0.0.0", "host to listen on")
	port := flag.Int("port", 7379, "port to listen on")
	walPath := flag.String("wal-path", "wal.log", "path to WAL file")
	expiryInterval := flag.Duration("expiry-interval", 100*time.Millisecond, "background TTL expiry scan interval")
	nodeID := flag.String("node-id", "node1", "unique node identifier")
	replMode := flag.String("replication-mode", "standalone", "replication mode: standalone, primary, replica")
	primaryAddr := flag.String("primary-addr", "", "address of primary's replication port (for replica mode)")
	replPort := flag.Int("replication-port", 7380, "TCP port for replication connections (primary mode)")
	clusterNodes := flag.String("cluster-nodes", "", "comma-separated nodeID=host:port pairs for cluster routing")
	flag.Parse()

	_ = clusterNodes // used by router; reserved for future cluster-routing integration

	ws, err := store.NewWALStoreWithRecovery(*walPath)
	if err != nil {
		slog.Error("failed to open WAL", "err", err)
		os.Exit(1)
	}

	storeCtx, storeCancel := context.WithCancel(context.Background())
	ws.StartExpiryLoop(storeCtx, *expiryInterval)

	cfg := server.Config{
		Host: *host,
		Port: *port,
	}

	var backend server.StoreBackend

	switch *replMode {
	case "primary":
		slog.Info("Starting as PRIMARY", "node-id", *nodeID, "replication-port", *replPort)
		p := replication.NewPrimary(ws, *replPort)
		if err := p.Start(); err != nil {
			slog.Error("failed to start primary replication listener", "err", err)
			os.Exit(1)
		}
		backend = server.NewPrimaryBackend(p)

	case "replica":
		if *primaryAddr == "" {
			slog.Error("replica mode requires -primary-addr")
			os.Exit(1)
		}
		slog.Info("Starting as REPLICA", "node-id", *nodeID, "primary", *primaryAddr)
		rep := replication.NewReplica(ws, *primaryAddr, *nodeID)
		if err := rep.StartSync(); err != nil {
			slog.Error("failed to connect replica to primary", "err", err)
			os.Exit(1)
		}
		cfg.ReadOnly = true
		backend = ws

	default: // standalone
		slog.Info("Starting in standalone mode", "node-id", *nodeID)
		backend = ws
	}

	srv := server.NewServer(cfg, backend)
	if err := srv.Start(); err != nil {
		slog.Error("failed to start server", "err", err)
		os.Exit(1)
	}

	sigs := []os.Signal{os.Interrupt}
	if runtime.GOOS != "windows" {
		sigs = append(sigs, syscall.SIGTERM)
	}
	sigCtx, stop := signal.NotifyContext(context.Background(), sigs...)
	defer stop()

	<-sigCtx.Done()

	slog.Info("Shutting down...")
	srv.Stop()
	storeCancel()
	ws.Close()
	slog.Info("Goodbye")
}
