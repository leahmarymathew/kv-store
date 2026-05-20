package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/leahmarymathew/kv-store/internal/protocol"
	"github.com/leahmarymathew/kv-store/internal/store"
)

func handleConnection(ctx context.Context, conn net.Conn, s *store.WALStore, wg *sync.WaitGroup, sem chan struct{}, cfg DeadlineConfig) {
	defer func() { <-sem }()
	defer conn.Close()
	defer wg.Done()

	bufReader := bufio.NewReaderSize(conn, 4096)
	bufWriter := bufio.NewWriterSize(conn, 4096)

	parser := protocol.NewParser(bufReader)
	serializer := protocol.NewSerializer(bufWriter)

	for {
		conn.SetDeadline(time.Now().Add(cfg.IdleTimeout))

		cmd, err := parser.Parse()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				slog.Debug("connection timed out", "remote", conn.RemoteAddr())
				return
			}
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("parse error", "remote", conn.RemoteAddr(), "err", err)
			return
		}

		resp := dispatch(cmd, s)

		conn.SetWriteDeadline(time.Now().Add(cfg.WriteTimeout))
		if err := serializer.Write(resp); err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				slog.Debug("write timed out", "remote", conn.RemoteAddr())
				return
			}
			slog.Error("write error", "remote", conn.RemoteAddr(), "err", err)
			return
		}
		if err := bufWriter.Flush(); err != nil {
			slog.Debug("flush error", "remote", conn.RemoteAddr(), "err", err)
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func dispatch(cmd *protocol.Command, s *store.WALStore) *protocol.Response {
	switch cmd.Type {
	case protocol.CmdGet:
		val, ok := s.Get(string(cmd.Key))
		if !ok {
			return protocol.NotFoundResponse()
		}
		return protocol.OKResponse(val)

	case protocol.CmdSet:
		if err := s.Set(string(cmd.Key), cmd.Value); err != nil {
			return protocol.ErrorResponse("write failed: " + err.Error())
		}
		return protocol.OKResponse(nil)

	case protocol.CmdDelete:
		found, err := s.Delete(string(cmd.Key))
		if err != nil {
			return protocol.ErrorResponse("write failed: " + err.Error())
		}
		if found {
			return protocol.OKResponse(nil)
		}
		return protocol.NotFoundResponse()

	case protocol.CmdTTL:
		if len(cmd.Value) != 8 {
			return protocol.ErrorResponse(fmt.Sprintf("TTL value must be 8 bytes, got %d", len(cmd.Value)))
		}
		secs := int64(binary.BigEndian.Uint64(cmd.Value))
		if err := s.SetWithTTL(string(cmd.Key), nil, time.Duration(secs)*time.Second); err != nil {
			return protocol.ErrorResponse("write failed: " + err.Error())
		}
		return protocol.OKResponse(nil)

	case protocol.CmdPing:
		return protocol.PongResponse()

	default:
		return protocol.ErrorResponse(fmt.Sprintf("unknown command: 0x%02x", cmd.Type))
	}
}
