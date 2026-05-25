# Build the server binary
Write-Host "Building server..."
go build -o server.exe ./cmd/server
if (-not $?) {
    Write-Error "Build failed"
    exit 1
}

# Start primary node
Write-Host "Starting primary node on :7379 (replication port :7380)..."
Start-Process -NoNewWindow .\server.exe -ArgumentList "-port 7379 -replication-mode primary -replication-port 7380 -wal-path wal_primary.log -node-id primary"

# Give the primary time to bind the replication port before replicas connect
Start-Sleep -Seconds 2

# Start replica 1
Write-Host "Starting replica 1 on :7381..."
Start-Process -NoNewWindow .\server.exe -ArgumentList "-port 7381 -replication-mode replica -primary-addr localhost:7380 -wal-path wal_replica1.log -node-id replica1"

# Start replica 2
Write-Host "Starting replica 2 on :7382..."
Start-Process -NoNewWindow .\server.exe -ArgumentList "-port 7382 -replication-mode replica -primary-addr localhost:7380 -wal-path wal_replica2.log -node-id replica2"

# Wait for replicas to connect and sync
Start-Sleep -Seconds 3

Write-Host "Cluster started:"
Write-Host "  Primary:   localhost:7379"
Write-Host "  Replica 1: localhost:7381"
Write-Host "  Replica 2: localhost:7382"
