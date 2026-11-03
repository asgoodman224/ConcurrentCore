# End-to-end demo for Windows: start the server in its own console, run every
# task type through the CLI client, print stats, then send CTRL+C so the
# server performs its graceful shutdown.
#   powershell -ExecutionPolicy Bypass -File scripts/demo.ps1 [-BuildDir build] [-Port 7878]
param([string]$BuildDir = "build", [int]$Port = 7878)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$bin = Join-Path $root "$BuildDir\bin"
$ws = Join-Path ([IO.Path]::GetTempPath()) ("cc-demo-" + [Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory $ws | Out-Null
Copy-Item (Join-Path $root "examples\workspace\*") $ws
$log = Join-Path $ws "server.log"
$server = Start-Process -FilePath (Join-Path $bin "concurrentcore-server.exe") `
    -ArgumentList "--port $Port --workspace `"$ws`"" -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput $log
$client = Join-Path $bin "concurrentcore-client.exe"
for ($i = 0; $i -lt 50; $i++) { & $client --port $Port ping *> $null; if ($LASTEXITCODE -eq 0) { break }; Start-Sleep -Milliseconds 100 }
& $client --port $Port ping
& $client --port $Port submit --type text-analysis --file sample.txt --wait
& $client --port $Port submit --type sha256 --file sample.txt --wait
& $client --port $Port submit --type compress --input sample.txt --format gzip --level 9 --wait
& $client --port $Port submit --type decompress --input sample.txt.gz --output roundtrip.txt --wait
& $client --port $Port submit --type compute --limit 2000000 --wait
& $client --port $Port stats
if ((Get-FileHash "$ws\sample.txt").Hash -eq (Get-FileHash "$ws\roundtrip.txt").Hash) { "round trip OK" }
& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "send_ctrl_c.ps1") -ProcessId $server.Id
if (-not $server.WaitForExit(15000)) { throw "server did not exit" }
"server exited with code $($server.ExitCode)"
Get-Content $log
Remove-Item -Recurse -Force $ws
