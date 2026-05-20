package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	StatusOK       = 0x00
	StatusNotFound = 0x01
	StatusError    = 0x02
	StatusPong     = 0x03
)

type Response struct {
	Status  byte
	Payload []byte
}

type Serializer struct {
	w io.Writer
}

func NewSerializer(w io.Writer) *Serializer {
	return &Serializer{w: w}
}

func (s *Serializer) Write(r *Response) error {
	if err := binary.Write(s.w, binary.BigEndian, r.Status); err != nil {
		return fmt.Errorf("writing status: %w", err)
	}
	if err := binary.Write(s.w, binary.BigEndian, uint32(len(r.Payload))); err != nil {
		return fmt.Errorf("writing payload length: %w", err)
	}
	if len(r.Payload) > 0 {
		if _, err := s.w.Write(r.Payload); err != nil {
			return fmt.Errorf("writing payload: %w", err)
		}
	}
	return nil
}

func OKResponse(payload []byte) *Response {
	return &Response{Status: StatusOK, Payload: payload}
}

func NotFoundResponse() *Response {
	return &Response{Status: StatusNotFound, Payload: nil}
}

func ErrorResponse(msg string) *Response {
	return &Response{Status: StatusError, Payload: []byte(msg)}
}

func PongResponse() *Response {
	return &Response{Status: StatusPong, Payload: nil}
}
