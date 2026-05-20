package server

import (
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/leahmarymathew/kv-store/internal/store"
)

type Config struct {
	Host     string
	Port     int
	MaxConns int
}

type Server struct {
	config   Config
	store    *store.Store
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan struct{}
}

func NewServer(cfg Config, s *store.Store) *Server {
	return &Server{
		config: cfg,
		store:  s,
		quit:   make(chan struct{}),
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
	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			select {
			case <-srv.quit:
				return
			default:
				slog.Error("accept error", "err", err)
				continue
			}
		}
		srv.wg.Add(1)
		go srv.handleConnection(conn)
	}
}

func (srv *Server) Stop() {
	close(srv.quit)
	srv.listener.Close()
	srv.wg.Wait()
	slog.Info("Server stopped")
}
