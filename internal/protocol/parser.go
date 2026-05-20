package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	CmdGet    = 0x01
	CmdSet    = 0x02
	CmdDelete = 0x03
	CmdTTL    = 0x04
	CmdPing   = 0x05
)

type Command struct {
	Type  byte
	Key   []byte
	Value []byte
}

type Parser struct {
	r io.Reader
}

func NewParser(r io.Reader) *Parser {
	return &Parser{r: r}
}

func (p *Parser) Parse() (*Command, error) {
	cmdBuf := make([]byte, 1)
	_, err := io.ReadFull(p.r, cmdBuf)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("reading command byte: %w", err)
	}

	var keyLen uint32
	if err := binary.Read(p.r, binary.BigEndian, &keyLen); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("reading key length: unexpected EOF")
		}
		return nil, fmt.Errorf("reading key length: %w", err)
	}

	key := make([]byte, keyLen)
	if keyLen > 0 {
		if _, err := io.ReadFull(p.r, key); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil, fmt.Errorf("reading key: unexpected EOF after %d of %d bytes", 0, keyLen)
			}
			return nil, fmt.Errorf("reading key: %w", err)
		}
	}

	var valLen uint32
	if err := binary.Read(p.r, binary.BigEndian, &valLen); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("reading value length: unexpected EOF")
		}
		return nil, fmt.Errorf("reading value length: %w", err)
	}

	value := make([]byte, valLen)
	if valLen > 0 {
		if _, err := io.ReadFull(p.r, value); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil, fmt.Errorf("reading value: unexpected EOF after %d of %d bytes", 0, valLen)
			}
			return nil, fmt.Errorf("reading value: %w", err)
		}
	}

	return &Command{
		Type:  cmdBuf[0],
		Key:   key,
		Value: value,
	}, nil
}
