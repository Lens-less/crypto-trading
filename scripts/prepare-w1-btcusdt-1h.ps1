[CmdletBinding()]
param(
    [string]$CacheDirectory = (Join-Path ([System.IO.Path]::GetTempPath()) 'crypto-trading-w1-btcusdt-1h-v1'),
    [string]$ProvenanceLock = (Join-Path (Split-Path -Parent $PSScriptRoot) 'artifacts\strategy-evaluation\w1-1h\w1-btcusdt-1h-provenance.tsv')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.IO.Compression.FileSystem

$protocolId = 'w1-btcusdt-spot-1h-20260812-v1'
$symbol = 'BTCUSDT'
$interval = '1h'
$firstMonth = [DateTimeOffset]::Parse('2018-01-01T00:00:00Z')
$monthAfterLast = [DateTimeOffset]::Parse('2026-08-01T00:00:00Z')
$expectedArchiveCount = 103
$baseUrl = 'https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1h'
$retrievedAt = [DateTimeOffset]::UtcNow.ToString('o')

function Get-Sha256Hex {
    param([byte[]]$Bytes)

    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        return ([System.BitConverter]::ToString($sha.ComputeHash($Bytes))).Replace('-', '').ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
    }
}

function Get-FileSha256Hex {
    param([string]$LiteralPath)

    return (Get-FileHash -LiteralPath $LiteralPath -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-UnixMicroseconds {
    param([DateTimeOffset]$Value)

    $epoch = [DateTimeOffset]::Parse('1970-01-01T00:00:00Z')
    return [int64](($Value.UtcTicks - $epoch.UtcTicks) / 10)
}

function Get-LockedRows {
    param([string]$LiteralPath)

    if (-not (Test-Path -LiteralPath $LiteralPath -PathType Leaf)) {
        return $null
    }

    $rows = @(Import-Csv -LiteralPath $LiteralPath -Delimiter "`t")
    if ($rows.Count -ne $expectedArchiveCount) {
        throw "Frozen provenance lock expected $expectedArchiveCount rows but contains $($rows.Count)."
    }
    return $rows
}

function Resolve-BinanceTimestampUnit {
    param(
        [int64]$ObservedFirstOpen,
        [int64]$ObservedLastClose,
        [DateTimeOffset]$MonthStart,
        [DateTimeOffset]$NextMonth
    )

    $expectedMillisecondsFirst = $MonthStart.ToUnixTimeMilliseconds()
    $expectedMillisecondsLast = $NextMonth.ToUnixTimeMilliseconds() - 1
    if ($ObservedFirstOpen -eq $expectedMillisecondsFirst -and $ObservedLastClose -eq $expectedMillisecondsLast) {
        return [pscustomobject]@{
            timestamp_unit = 'milliseconds'
            expected_first_open = $MonthStart.ToString("yyyy-MM-ddTHH:mm:ss.fff'Z'")
            expected_last_close = $NextMonth.AddMilliseconds(-1).ToString("yyyy-MM-ddTHH:mm:ss.fff'Z'")
        }
    }

    $expectedMicrosecondsFirst = Get-UnixMicroseconds -Value $MonthStart
    $expectedMicrosecondsLast = (Get-UnixMicroseconds -Value $NextMonth) - 1
    if ($ObservedFirstOpen -eq $expectedMicrosecondsFirst -and $ObservedLastClose -eq $expectedMicrosecondsLast) {
        return [pscustomobject]@{
            timestamp_unit = 'microseconds'
            expected_first_open = $MonthStart.ToString("yyyy-MM-ddTHH:mm:ss.ffffff'Z'")
            expected_last_close = $NextMonth.AddTicks(-10).ToString("yyyy-MM-ddTHH:mm:ss.ffffff'Z'")
        }
    }

    throw "Archive endpoints do not match supported Binance timestamp units for $($MonthStart.ToString('yyyy-MM'))."
}

$resolvedCache = [System.IO.Path]::GetFullPath($CacheDirectory)
$resolvedLock = [System.IO.Path]::GetFullPath($ProvenanceLock)
New-Item -ItemType Directory -Path $resolvedCache -Force | Out-Null
New-Item -ItemType Directory -Path (Split-Path -Parent $resolvedLock) -Force | Out-Null

$lockedRows = Get-LockedRows -LiteralPath $resolvedLock
$lockedByUrl = @{}
if ($null -ne $lockedRows) {
    foreach ($row in $lockedRows) {
        if ($lockedByUrl.ContainsKey($row.source_url)) {
            throw "Duplicate source URL in frozen provenance lock: $($row.source_url)"
        }
        $lockedByUrl.Add($row.source_url, $row)
    }
}

$outputRows = [System.Collections.Generic.List[object]]::new()
for ($month = $firstMonth; $month -lt $monthAfterLast; $month = $month.AddMonths(1)) {
    $monthLabel = $month.ToString('yyyy-MM')
    $fileName = "$symbol-$interval-$monthLabel.zip"
    $csvName = "$symbol-$interval-$monthLabel.csv"
    $sourceUrl = "$baseUrl/$fileName"
    $checksumUrl = "$sourceUrl.CHECKSUM"
    $zipPath = Join-Path $resolvedCache $fileName
    $csvPath = Join-Path $resolvedCache $csvName

    $checksumResponse = Invoke-WebRequest -Uri $checksumUrl -UseBasicParsing
    $checksumBody = if ($checksumResponse.Content -is [byte[]]) {
        [System.Text.Encoding]::UTF8.GetString($checksumResponse.Content)
    }
    else {
        [string]$checksumResponse.Content
    }
    $checksumText = $checksumBody.Trim()
    $checksumMatch = [regex]::Match($checksumText, '^(?<sha>[0-9a-fA-F]{64})\s+')
    if (-not $checksumMatch.Success) {
        throw "Malformed official checksum response: $checksumUrl"
    }
    $officialSha = $checksumMatch.Groups['sha'].Value.ToLowerInvariant()

    if ($lockedByUrl.ContainsKey($sourceUrl)) {
        $lockedSha = $lockedByUrl[$sourceUrl].archive_sha256.ToLowerInvariant()
        if ($officialSha -ne $lockedSha) {
            throw "Official archive changed after freeze: $sourceUrl"
        }
    }

    if (-not (Test-Path -LiteralPath $zipPath -PathType Leaf)) {
        Invoke-WebRequest -Uri $sourceUrl -OutFile $zipPath -UseBasicParsing
    }
    $observedSha = Get-FileSha256Hex -LiteralPath $zipPath
    if ($observedSha -ne $officialSha) {
        throw "Archive SHA-256 mismatch: $sourceUrl"
    }

    $zip = [System.IO.Compression.ZipFile]::OpenRead($zipPath)
    try {
        $entries = @($zip.Entries | Where-Object { -not [string]::IsNullOrEmpty($_.Name) })
        if ($entries.Count -ne 1 -or $entries[0].FullName -ne $csvName) {
            throw "Archive must contain exactly ${csvName}: $sourceUrl"
        }
        $memory = [System.IO.MemoryStream]::new()
        try {
            $entryStream = $entries[0].Open()
            try {
                $entryStream.CopyTo($memory)
            }
            finally {
                $entryStream.Dispose()
            }
            $csvBytes = $memory.ToArray()
        }
        finally {
            $memory.Dispose()
        }
    }
    finally {
        $zip.Dispose()
    }

    $contentSha = Get-Sha256Hex -Bytes $csvBytes
    if (Test-Path -LiteralPath $csvPath -PathType Leaf) {
        if ((Get-FileSha256Hex -LiteralPath $csvPath) -ne $contentSha) {
            throw "Cached CSV differs from verified archive and will not be overwritten: $csvPath"
        }
    }
    else {
        [System.IO.File]::WriteAllBytes($csvPath, $csvBytes)
    }

    $csvText = [System.Text.Encoding]::UTF8.GetString($csvBytes)
    $lines = @($csvText -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $nextMonth = $month.AddMonths(1)
    $expectedRows = [int](($nextMonth - $month).TotalDays * 24)
    if ($lines.Count -ne $expectedRows) {
        throw "Archive $sourceUrl expected $expectedRows hourly rows but contains $($lines.Count)."
    }

    $firstFields = $lines[0].Split(',')
    $lastFields = $lines[-1].Split(',')
    if ($firstFields.Count -ne 12 -or $lastFields.Count -ne 12) {
        throw "Archive endpoint rows do not have the 12-column kline shape: $sourceUrl"
    }

    $resolvedUnit = Resolve-BinanceTimestampUnit `
        -ObservedFirstOpen ([int64]$firstFields[0]) `
        -ObservedLastClose ([int64]$lastFields[6]) `
        -MonthStart $month `
        -NextMonth $nextMonth

    $row = [pscustomobject][ordered]@{
        source_url = $sourceUrl
        retrieved_at = $retrievedAt
        archive_sha256 = $officialSha
        observed_archive_sha256 = $observedSha
        content_sha256 = $contentSha
        timestamp_unit = $resolvedUnit.timestamp_unit
        expected_first_open = $resolvedUnit.expected_first_open
        expected_last_close = $resolvedUnit.expected_last_close
        expected_bar_count = $expectedRows
        csv_file = $csvName
    }
    if ($lockedByUrl.ContainsKey($sourceUrl)) {
        $locked = $lockedByUrl[$sourceUrl]
        foreach ($field in @('archive_sha256', 'observed_archive_sha256', 'content_sha256', 'timestamp_unit', 'expected_first_open', 'expected_last_close', 'expected_bar_count', 'csv_file')) {
            if ([string]$row.$field -ne [string]$locked.$field) {
                throw "Cached source differs from frozen provenance field '$field': $sourceUrl"
            }
        }
        $row.retrieved_at = $locked.retrieved_at
    }
    $outputRows.Add($row)
}

if ($outputRows.Count -ne $expectedArchiveCount) {
    throw "Expected $expectedArchiveCount archives but prepared $($outputRows.Count)."
}

$rendered = $outputRows | ConvertTo-Csv -Delimiter "`t" -NoTypeInformation
if ($null -eq $lockedRows) {
    [System.IO.File]::WriteAllLines($resolvedLock, $rendered, [System.Text.UTF8Encoding]::new($false))
}
else {
    $existing = [System.IO.File]::ReadAllLines($resolvedLock)
    if ([string]::Join("`n", $existing) -ne [string]::Join("`n", $rendered)) {
        throw 'Prepared provenance differs from the existing frozen lock.'
    }
}

[pscustomobject]@{
    protocol = $protocolId
    cache_directory = $resolvedCache
    provenance_lock = $resolvedLock
    archive_count = $outputRows.Count
    first_month = '2018-01'
    last_month = '2026-07'
    interval = $interval
}
