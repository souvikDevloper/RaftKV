param(
    [switch]$PreserveData,
    [string]$Go = "go"
)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Run = Join-Path $Root "run"
$Data = Join-Path $Root "data"
New-Item -ItemType Directory -Force -Path $Run, $Data | Out-Null

& (Join-Path $PSScriptRoot "stop_cluster.ps1")
if (-not $PreserveData) {
    1..5 | ForEach-Object {
        $path = Join-Path $Data "n$_"
        if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path -Recurse -Force }
    }
}

$Binary = Join-Path $Run "raftkv.exe"
& $Go build -trimpath -o $Binary ./cmd/raftkv
if ($LASTEXITCODE -ne 0) { throw "go build failed" }

$Nodes = (1..5 | ForEach-Object { "127.0.0.1:700$_" }) -join ","
1..5 | ForEach-Object {
    $id = $_
    $peers = (1..5 | Where-Object { $_ -ne $id } | ForEach-Object { "n$_=127.0.0.1:700$_" }) -join ","
    $arguments = @("server", "--id", "n$id", "--listen", "127.0.0.1:700$id", "--metrics-listen", "127.0.0.1:910$id", "--peers", $peers, "--data", (Join-Path $Data "n$id"))
    if ($id -eq 1) { $arguments += @("--resp-listen", "127.0.0.1:6380") }
    $process = Start-Process -FilePath $Binary -ArgumentList $arguments -PassThru -WindowStyle Hidden -RedirectStandardOutput (Join-Path $Run "n$id.stdout.log") -RedirectStandardError (Join-Path $Run "n$id.stderr.log")
    Set-Content -LiteralPath (Join-Path $Run "n$id.pid") -Value $process.Id
}

for ($attempt = 0; $attempt -lt 80; $attempt++) {
    $status = & $Binary status --nodes $Nodes 2>$null | Out-String
    if ($status -match '"role":"leader"') {
        Write-Output "RaftKV 5-node cluster ready; YCSB endpoint: 127.0.0.1:6380"
        Write-Output $status.Trim()
        return
    }
    Start-Sleep -Milliseconds 100
}
throw "cluster did not elect a leader; inspect run/*.stderr.log"
