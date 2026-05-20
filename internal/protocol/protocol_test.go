package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encodeCommand builds a raw request frame into a bytes.Buffer.
func encodeCommand(cmdType byte, key, value []byte) *bytes.Buffer {
	buf := &bytes.Buffer{}
	buf.WriteByte(cmdType)
	_ = binary.Write(buf, binary.BigEndian, uint32(len(key)))
	buf.Write(key)
	_ = binary.Write(buf, binary.BigEndian, uint32(len(value)))
	buf.Write(value)
	return buf
}

// --- Parser tests ---

func TestParseGet(t *testing.T) {
	key := []byte("hello")
	buf := encodeCommand(CmdGet, key, nil)

	cmd, err := NewParser(buf).Parse()
	require.NoError(t, err)
	assert.Equal(t, byte(CmdGet), cmd.Type)
	assert.Equal(t, key, cmd.Key)
	assert.Empty(t, cmd.Value)
}

func TestParseSet(t *testing.T) {
	key := []byte("name")
	value := []byte("leah")
	buf := encodeCommand(CmdSet, key, value)

	cmd, err := NewParser(buf).Parse()
	require.NoError(t, err)
	assert.Equal(t, byte(CmdSet), cmd.Type)
	assert.Equal(t, key, cmd.Key)
	assert.Equal(t, value, cmd.Value)
}

func TestParsePing(t *testing.T) {
	buf := encodeCommand(CmdPing, nil, nil)

	cmd, err := NewParser(buf).Parse()
	require.NoError(t, err)
	assert.Equal(t, byte(CmdPing), cmd.Type)
	assert.Empty(t, cmd.Key)
	assert.Empty(t, cmd.Value)
}

func TestParseEmptyReader(t *testing.T) {
	_, err := NewParser(bytes.NewReader(nil)).Parse()
	assert.ErrorIs(t, err, io.EOF)
}

func TestParseShortRead(t *testing.T) {
	// Only 3 bytes: command byte + 2 bytes of key length (needs 4).
	_, err := NewParser(bytes.NewReader([]byte{CmdGet, 0x00, 0x00})).Parse()
	assert.True(t, errors.Is(err, io.ErrUnexpectedEOF), "expected ErrUnexpectedEOF, got: %v", err)
}

// --- Serializer tests ---

func TestWriteOK(t *testing.T) {
	payload := []byte("value123")
	buf := &bytes.Buffer{}

	err := NewSerializer(buf).Write(OKResponse(payload))
	require.NoError(t, err)

	b := buf.Bytes()
	assert.Equal(t, byte(StatusOK), b[0])
	length := binary.BigEndian.Uint32(b[1:5])
	assert.Equal(t, uint32(len(payload)), length)
	assert.Equal(t, payload, b[5:])
}

func TestWriteNotFound(t *testing.T) {
	buf := &bytes.Buffer{}

	err := NewSerializer(buf).Write(NotFoundResponse())
	require.NoError(t, err)

	b := buf.Bytes()
	assert.Equal(t, byte(StatusNotFound), b[0])
	length := binary.BigEndian.Uint32(b[1:5])
	assert.Equal(t, uint32(0), length)
	assert.Equal(t, 5, len(b))
}

func TestRoundTrip(t *testing.T) {
	original := OKResponse([]byte("round-trip-value"))
	buf := &bytes.Buffer{}

	err := NewSerializer(buf).Write(original)
	require.NoError(t, err)

	// Read back manually: status + length + payload.
	raw := buf.Bytes()
	status := raw[0]
	length := binary.BigEndian.Uint32(raw[1:5])
	payload := raw[5 : 5+length]

	assert.Equal(t, original.Status, status)
	assert.Equal(t, uint32(len(original.Payload)), length)
	assert.Equal(t, original.Payload, payload)
}
