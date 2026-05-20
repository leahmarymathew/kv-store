package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/leahmarymathew/kv-store/internal/protocol"
	"github.com/leahmarymathew/kv-store/internal/store"
)

func (srv *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer srv.wg.Done()

	parser := protocol.NewParser(conn)
	serializer := protocol.NewSerializer(conn)

	for {
		cmd, err := parser.Parse()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("parse error", "remote", conn.RemoteAddr(), "err", err)
			return
		}

		resp := dispatch(cmd, srv.store)
		if err := serializer.Write(resp); err != nil {
			slog.Error("write error", "remote", conn.RemoteAddr(), "err", err)
			return
		}
	}
}

func dispatch(cmd *protocol.Command, s *store.Store) *protocol.Response {
	switch cmd.Type {
	case protocol.CmdGet:
		val, ok := s.Get(string(cmd.Key))
		if !ok {
			return protocol.NotFoundResponse()
		}
		return protocol.OKResponse(val)

	case protocol.CmdSet:
		s.Set(string(cmd.Key), cmd.Value)
		return protocol.OKResponse(nil)

	case protocol.CmdDelete:
		if s.Delete(string(cmd.Key)) {
			return protocol.OKResponse(nil)
		}
		return protocol.NotFoundResponse()

	case protocol.CmdTTL:
		if len(cmd.Value) != 8 {
			return protocol.ErrorResponse(fmt.Sprintf("TTL value must be 8 bytes, got %d", len(cmd.Value)))
		}
		secs := int64(binary.BigEndian.Uint64(cmd.Value))
		s.SetWithTTL(string(cmd.Key), nil, time.Duration(secs)*time.Second)
		return protocol.OKResponse(nil)

	case protocol.CmdPing:
		return protocol.PongResponse()

	default:
		return protocol.ErrorResponse(fmt.Sprintf("unknown command: 0x%02x", cmd.Type))
	}
}
