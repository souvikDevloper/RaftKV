param(
    [int[]]$ThreadCounts = @(1, 2, 4, 8, 16, 32),
    [int]$RecordCount = 1000,
    [int]$OperationCount = 100000
)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$Rows = @()
foreach ($threads in $ThreadCounts) {
    $output = & (Join-Path $PSScriptRoot "run.ps1") -Workload c -RecordCount $RecordCount -OperationCount $OperationCount -Threads $threads -FieldCount 1 -FieldLength 100 2>&1
    $output | Write-Output
    $resultLine = $output | ForEach-Object { $_.ToString() } | Where-Object { $_ -match '^Results:\s+' } | Select-Object -Last 1
    if (-not $resultLine) { throw "YCSB result directory missing for thread count $threads" }
    $directory = ($resultLine -replace '^Results:\s+', '').Trim()
    $summary = Get-Content -Raw -LiteralPath (Join-Path $directory "summary.json") | ConvertFrom-Json
    $metrics = $summary.workloads.workloadc.metrics
    $Rows += [pscustomobject]@{
        threads = $threads
        throughput_ops_sec = $metrics.OVERALL.'Throughput(ops/sec)'
        read_average_us = $metrics.READ.'AverageLatency(us)'
        read_p95_us = $metrics.READ.'95thPercentileLatency(us)'
        read_p99_us = $metrics.READ.'99thPercentileLatency(us)'
        meets_resume_gate = ($metrics.OVERALL.'Throughput(ops/sec)' -ge 18000 -and $metrics.READ.'99thPercentileLatency(us)' -lt 3000)
        evidence_directory = $directory
    }
}
$timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$target = Join-Path $Root "benchmark-results\ycsb-curve-$timestamp.csv"
$Rows | Export-Csv -NoTypeInformation -LiteralPath $target
$Rows | Format-Table -AutoSize
Write-Output "Throughput/latency curve: $target"
