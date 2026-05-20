# KV Store Wire Protocol

## Request Format
[1 byte command][4 bytes key length BE][N bytes key][4 bytes value length BE][M bytes value]

## Commands
0x01 = GET   (value length = 0, no value bytes)
0x02 = SET   (key + value)
0x03 = DELETE (value length = 0)
0x04 = TTL   (value = expiry seconds as 8-byte big-endian int64)
0x05 = PING  (key length = 0, value length = 0)

## Response Format
[1 byte status][4 bytes payload length BE][N bytes payload]

## Status Codes
0x00 = OK
0x01 = NOT_FOUND
0x02 = ERROR
0x03 = PONG
