Get-Process | Where-Object {$_.ProcessName -eq "server"} | Stop-Process
Write-Host "All server processes stopped."
