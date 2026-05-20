package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/leahmarymathew/kv-store/internal/protocol"
)

func main() {
	// Manual flag parsing to keep import list minimal.
	host := "localhost"
	port := 7379
	for i, arg := range os.Args[1:] {
		switch arg {
		case "-host", "--host":
			if i+1 < len(os.Args[1:]) {
				host = os.Args[i+2]
			}
		case "-port", "--port":
			if i+1 < len(os.Args[1:]) {
				p, err := strconv.Atoi(os.Args[i+2])
				if err == nil {
					port = p
				}
			}
		}
	}

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Printf("Connected to %s\n", addr)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			if err := writeCommand(conn, protocol.CmdGet, parts[1], ""); err != nil {
				fmt.Fprintf(os.Stderr, "send error: %v\n", err)
				return
			}
			printResponse(conn)

		case "set":
			if len(parts) < 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			if err := writeCommand(conn, protocol.CmdSet, parts[1], parts[2]); err != nil {
				fmt.Fprintf(os.Stderr, "send error: %v\n", err)
				return
			}
			printResponse(conn)

		case "delete":
			if len(parts) < 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			if err := writeCommand(conn, protocol.CmdDelete, parts[1], ""); err != nil {
				fmt.Fprintf(os.Stderr, "send error: %v\n", err)
				return
			}
			printResponse(conn)

		case "ttl":
			if len(parts) < 3 {
				fmt.Println("Usage: ttl <key> <seconds>")
				continue
			}
			secs, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "invalid seconds: %v\n", err)
				continue
			}
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(secs))
			if err := writeCommandBytes(conn, protocol.CmdTTL, []byte(parts[1]), val); err != nil {
				fmt.Fprintf(os.Stderr, "send error: %v\n", err)
				return
			}
			printResponse(conn)

		case "ping":
			if err := writeCommand(conn, protocol.CmdPing, "", ""); err != nil {
				fmt.Fprintf(os.Stderr, "send error: %v\n", err)
				return
			}
			printResponse(conn)

		case "quit", "exit":
			fmt.Println("Bye!")
			return

		default:
			fmt.Println("Unknown command. Supported: get, set, delete, ttl, ping, quit")
		}
	}
}

// writeCommand sends a command with string key and value.
func writeCommand(conn net.Conn, cmdType byte, key, value string) error {
	return writeCommandBytes(conn, cmdType, []byte(key), []byte(value))
}

// writeCommandBytes encodes and sends a binary protocol request.
func writeCommandBytes(conn net.Conn, cmdType byte, key, value []byte) error {
	if _, err := conn.Write([]byte{cmdType}); err != nil {
		return err
	}
	if err := binary.Write(conn, binary.BigEndian, uint32(len(key))); err != nil {
		return err
	}
	if len(key) > 0 {
		if _, err := conn.Write(key); err != nil {
			return err
		}
	}
	if err := binary.Write(conn, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	if len(value) > 0 {
		if _, err := conn.Write(value); err != nil {
			return err
		}
	}
	return nil
}

// ReadResponse reads [1 byte status][4 bytes length BE][N bytes payload].
func ReadResponse(r io.Reader) (status byte, payload []byte, err error) {
	statusBuf := make([]byte, 1)
	if _, err = io.ReadFull(r, statusBuf); err != nil {
		return 0, nil, fmt.Errorf("reading status: %w", err)
	}
	status = statusBuf[0]

	var length uint32
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return 0, nil, fmt.Errorf("reading payload length: %w", err)
	}

	payload = make([]byte, length)
	if length > 0 {
		if _, err = io.ReadFull(r, payload); err != nil {
			return 0, nil, fmt.Errorf("reading payload: %w", err)
		}
	}
	return status, payload, nil
}

func printResponse(r io.Reader) {
	status, payload, err := ReadResponse(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read error: %v\n", err)
		return
	}
	switch status {
	case protocol.StatusOK:
		if len(payload) > 0 {
			fmt.Printf("OK: %s\n", payload)
		} else {
			fmt.Println("OK")
		}
	case protocol.StatusNotFound:
		fmt.Println("NOT FOUND")
	case protocol.StatusError:
		fmt.Printf("ERROR: %s\n", payload)
	case protocol.StatusPong:
		fmt.Println("PONG")
	default:
		fmt.Printf("UNKNOWN STATUS 0x%02x: %s\n", status, payload)
	}
}
