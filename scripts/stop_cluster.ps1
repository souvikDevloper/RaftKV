$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Run = Join-Path $Root "run"
if (-not (Test-Path -LiteralPath $Run)) { exit 0 }
Get-ChildItem -LiteralPath $Run -Filter "*.pid" -File | ForEach-Object {
    $pidValue = (Get-Content -LiteralPath $_.FullName -Raw).Trim()
    if ($pidValue -match '^\d+$') {
        Stop-Process -Id ([int]$pidValue) -Force -ErrorAction SilentlyContinue
    }
    Remove-Item -LiteralPath $_.FullName -Force
}
