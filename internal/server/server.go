package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/leahmarymathew/kv-store/internal/store"
)

type DeadlineConfig struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

type Config struct {
	Host      string
	Port      int
	MaxConns  int
	Deadlines DeadlineConfig
}

type Server struct {
	config     Config
	store      *store.Store
	listener   net.Listener
	wg         sync.WaitGroup
	sem        chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	acceptDone chan struct{} // closed when acceptLoop exits
}

func NewServer(cfg Config, s *store.Store) *Server {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 1000
	}
	if cfg.Deadlines.ReadTimeout <= 0 {
		cfg.Deadlines.ReadTimeout = 30 * time.Second
	}
	if cfg.Deadlines.WriteTimeout <= 0 {
		cfg.Deadlines.WriteTimeout = 10 * time.Second
	}
	if cfg.Deadlines.IdleTimeout <= 0 {
		cfg.Deadlines.IdleTimeout = 5 * time.Minute
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		config:     cfg,
		store:      s,
		sem:        make(chan struct{}, cfg.MaxConns),
		ctx:        ctx,
		cancel:     cancel,
		acceptDone: make(chan struct{}),
	}
}

func (srv *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", srv.config.Host, srv.config.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}
	srv.listener = ln
	slog.Info("Server listening", "addr", addr)
	go srv.acceptLoop()
	return nil
}

func (srv *Server) acceptLoop() {
	defer close(srv.acceptDone)
	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			select {
			case <-srv.ctx.Done():
				return
			default:
				slog.Error("accept error", "err", err)
				continue
			}
		}
		select {
		case srv.sem <- struct{}{}:
			// semaphore acquired, proceed
		default:
			conn.Close()
			slog.Warn("max connections reached, closing new connection")
			continue
		}
		srv.wg.Add(1)
		go handleConnection(srv.ctx, conn, srv.store, &srv.wg, srv.sem, srv.config.Deadlines)
	}
}

func (srv *Server) Stop() {
	srv.cancel()
	srv.listener.Close()

	// Wait for acceptLoop to exit before draining connections.
	// This guarantees no wg.Add(1) calls happen concurrently with wg.Wait(),
	// which would violate sync.WaitGroup's documented usage.
	<-srv.acceptDone

	done := make(chan struct{})
	go func() {
		srv.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("all connections drained")
	case <-time.After(30 * time.Second):
		slog.Warn("shutdown timeout, forcing close")
	}

	slog.Info("Server stopped")
}

func (srv *Server) ActiveConnections() int {
	return len(srv.sem)
}
