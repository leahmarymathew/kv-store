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

	"github.com/leahmarymathew/kv-store/internal/server"
	"github.com/leahmarymathew/kv-store/internal/store"
)

func main() {
	host := flag.String("host", "0.0.0.0", "host to listen on")
	port := flag.Int("port", 7379, "port to listen on")
	expiryInterval := flag.Duration("expiry-interval", 100*time.Millisecond, "background TTL expiry scan interval")
	flag.Parse()

	s := store.NewStore()

	storeCtx, storeCancel := context.WithCancel(context.Background())
	s.StartExpiryLoop(storeCtx, *expiryInterval)

	cfg := server.Config{
		Host: *host,
		Port: *port,
	}
	srv := server.NewServer(cfg, s)
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
	slog.Info("Goodbye")
}
