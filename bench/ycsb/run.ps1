param(
    [ValidateSet("a", "b", "c", "d", "e", "f", "all")][string]$Workload = "all",
    [int]$RecordCount = 100000,
    [int]$OperationCount = 100000,
    [int]$Threads = 64,
    [int]$Target = 0,
    [int]$FieldCount = 1,
    [int]$FieldLength = 100
)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$Tools = Join-Path $Root ".tools"
$Version = "0.17.0"
$Ycsb = Join-Path $Tools "YCSB-$Version"
$MavenVersion = "3.9.11"
$Maven = Join-Path $Tools "apache-maven-$MavenVersion"
New-Item -ItemType Directory -Force -Path $Tools | Out-Null
if (-not $env:JAVA_HOME) {
    $javaHomeLine = & cmd.exe /c "java -XshowSettings:properties -version 2>&1" | Select-String -Pattern '^\s*java\.home\s*=' | Select-Object -First 1
    if (-not $javaHomeLine) { throw "JAVA_HOME is unset and java.home could not be discovered" }
    $env:JAVA_HOME = ($javaHomeLine.ToString() -split '=', 2)[1].Trim()
}
if (-not (Test-Path -LiteralPath $Ycsb)) {
    git clone --depth 1 --branch $Version https://github.com/brianfrankcooper/YCSB.git $Ycsb
}
if (-not (Test-Path -LiteralPath $Maven)) {
    $archive = Join-Path $Tools "apache-maven-$MavenVersion-bin.zip"
    if (-not (Test-Path -LiteralPath $archive)) {
        & curl.exe -L --fail --retry 5 -o $archive "https://archive.apache.org/dist/maven/maven-3/$MavenVersion/binaries/apache-maven-$MavenVersion-bin.zip"
        if ($LASTEXITCODE -ne 0) { throw "Maven download failed" }
    }
    Expand-Archive -LiteralPath $archive -DestinationPath $Tools -Force
}
$Runner = Join-Path $Ycsb "bin\ycsb.bat"
if (-not (Test-Path -LiteralPath (Join-Path $Ycsb "redis\target\redis-binding-$Version.jar")) -or -not (Test-Path -LiteralPath (Join-Path $Ycsb "core\target\dependency"))) {
    & (Join-Path $Maven "bin\mvn.cmd") -f (Join-Path $Ycsb "pom.xml") -Psource-run -pl site.ycsb:redis-binding -am package -DskipTests
    if ($LASTEXITCODE -ne 0) { throw "YCSB Redis binding build failed" }
}
# YCSB 0.17.0 pins HdrHistogram 2.1.4, which references JAXB removed from
# modern JDKs. Supplying the API jar preserves the official YCSB code while
# keeping the harness runnable on Java 9+.
$Jaxb = Join-Path $Ycsb "core\target\dependency\jaxb-api-2.3.1.jar"
if (-not (Test-Path -LiteralPath $Jaxb)) {
    & (Join-Path $Maven "bin\mvn.cmd") dependency:copy "-Dartifact=javax.xml.bind:jaxb-api:2.3.1" "-DoutputDirectory=$(Split-Path -Parent $Jaxb)"
    if ($LASTEXITCODE -ne 0) { throw "JAXB compatibility dependency download failed" }
}

$Timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$Results = Join-Path $Root "benchmark-results\$Timestamp"
New-Item -ItemType Directory -Force -Path $Results | Out-Null
$manifest = [ordered]@{
    timestamp_utc = $Timestamp
    git_commit = (git -C $Root rev-parse HEAD).Trim()
    dirty = [bool](git -C $Root status --porcelain)
    os = [System.Environment]::OSVersion.VersionString
    cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name)
    logical_processors = [System.Environment]::ProcessorCount
    memory_bytes = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
    java = (& cmd.exe /c "java -version 2>&1" | Select-Object -First 1 | Out-String).Trim()
    ycsb_version = $Version
    record_count = $RecordCount
    operation_count = $OperationCount
    threads = $Threads
    target_ops_per_sec = $Target
    field_count = $FieldCount
    field_length_bytes = $FieldLength
    endpoint = "127.0.0.1:6380"
}
$manifest | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $Results "manifest.json") -Encoding utf8

$Common = @("-p", "redis.host=127.0.0.1", "-p", "redis.port=6380", "-p", "recordcount=$RecordCount", "-p", "operationcount=$OperationCount", "-p", "threadcount=$Threads", "-p", "fieldcount=$FieldCount", "-p", "fieldlength=$FieldLength", "-p", "measurementtype=hdrhistogram", "-p", "hdrhistogram.fileoutput=true")
if ($Target -gt 0) { $Common += @("-target", "$Target") }

# Load exactly once. Workloads A-F then operate on the same keyspace, matching
# YCSB's intended workflow and avoiding distorted distributions.
$ErrorActionPreference = "Continue"
& $Runner load redis -s -P (Join-Path $Ycsb "workloads\workloada") @Common -p "hdrhistogram.output.path=$(Join-Path $Results 'load-')" 2>&1 | Tee-Object -FilePath (Join-Path $Results "load.txt")
$loadExit = $LASTEXITCODE
$ErrorActionPreference = "Stop"
if ($loadExit -ne 0) { throw "YCSB load failed" }
$FailurePattern = 'Exception in thread|NoClassDefFoundError|JedisDataException|Return=ERROR'
if (Select-String -Path (Join-Path $Results "load.txt") -Pattern $FailurePattern -Quiet) { throw "YCSB load emitted an operation or JVM error" }

$selected = if ($Workload -eq "all") { @("a", "b", "c", "d", "e", "f") } else { @($Workload) }
foreach ($name in $selected) {
    1..5 | ForEach-Object { try { Invoke-WebRequest -Method Post "http://127.0.0.1:910$_/debug/histograms" -UseBasicParsing | Out-Null } catch {} }
    $ErrorActionPreference = "Continue"
    & $Runner run redis -s -P (Join-Path $Ycsb "workloads\workload$name") @Common -p "hdrhistogram.output.path=$(Join-Path $Results "workload$name-")" 2>&1 | Tee-Object -FilePath (Join-Path $Results "workload$name-run.txt")
    $runExit = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    if ($runExit -ne 0) { throw "YCSB workload $name failed" }
    if (Select-String -Path (Join-Path $Results "workload$name-run.txt") -Pattern $FailurePattern -Quiet) { throw "YCSB workload $name emitted an operation or JVM error" }
    1..5 | ForEach-Object { try { Invoke-WebRequest "http://127.0.0.1:910$_/debug/histograms" -UseBasicParsing -OutFile (Join-Path $Results "workload$name-node$_.json") } catch {} }
}

python (Join-Path $PSScriptRoot "summarize.py") $Results
Write-Output "Results: $Results"
