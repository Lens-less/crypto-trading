[CmdletBinding()]
param(
    [string]$CacheDirectory = (Join-Path ([System.IO.Path]::GetTempPath()) 'crypto-trading-w1-btcusdt-1h-v1'),
    [string]$ProvenanceLock,
    [string]$ExpectedArtifact,
    [string]$OutputPath,
    [switch]$FetchMissingChecksums,
    [switch]$SelfTest,
    [string]$ProtocolId = 'w1-btcusdt-spot-1h-20260812-v1',
    [string]$FrozenPreregistrationCommit = '855d0fd8652db74e7c18de393e2d78f3abbeae5d',
    [string]$SourceBaseUrl = 'https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1h',
    [DateTimeOffset]$FirstMonth = ([DateTimeOffset]::Parse('2018-01-01T00:00:00Z')),
    [DateTimeOffset]$MonthAfterLast = ([DateTimeOffset]::Parse('2026-08-01T00:00:00Z'))
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.IO.Compression.FileSystem
Add-Type -AssemblyName System.IO.Compression

$HourMicroseconds = 3600000000L
$HourMilliseconds = 3600000L
$ExpectedColumns = 12

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

function Convert-RawTimestampToMicroseconds {
    param(
        [int64]$RawValue,
        [string]$TimestampUnit
    )

    if ($TimestampUnit -eq 'milliseconds') {
        return $RawValue * 1000L
    }

    return $RawValue
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
            hour_width_raw = $HourMilliseconds
        }
    }

    $expectedMicrosecondsFirst = Get-UnixMicroseconds -Value $MonthStart
    $expectedMicrosecondsLast = (Get-UnixMicroseconds -Value $NextMonth) - 1
    if ($ObservedFirstOpen -eq $expectedMicrosecondsFirst -and $ObservedLastClose -eq $expectedMicrosecondsLast) {
        return [pscustomobject]@{
            timestamp_unit = 'microseconds'
            hour_width_raw = $HourMicroseconds
        }
    }

    throw "Archive endpoints do not match supported Binance timestamp units for $($MonthStart.ToString('yyyy-MM'))."
}

function Get-MonthRegistry {
    param(
        [DateTimeOffset]$StartMonth,
        [DateTimeOffset]$ExclusiveEndMonth,
        [string]$Symbol,
        [string]$Interval,
        [string]$BaseUrl
    )

    $months = [System.Collections.Generic.List[object]]::new()
    for ($month = $StartMonth; $month -lt $ExclusiveEndMonth; $month = $month.AddMonths(1)) {
        $monthLabel = $month.ToString('yyyy-MM')
        $nextMonth = $month.AddMonths(1)
        $fileName = "$Symbol-$Interval-$monthLabel.zip"
        $csvName = "$Symbol-$Interval-$monthLabel.csv"
        $months.Add([pscustomobject]@{
            label = $monthLabel
            start = $month
            next = $nextMonth
            expected_rows = [int](($nextMonth - $month).TotalDays * 24)
            zip_name = $fileName
            csv_name = $csvName
            source_url = "$BaseUrl/$fileName"
            checksum_url = "$BaseUrl/$fileName.CHECKSUM"
        })
    }

    return $months
}

function Get-LockedRowsByUrl {
    param(
        [string]$LiteralPath,
        [int]$ExpectedCount
    )

    if (-not (Test-Path -LiteralPath $LiteralPath -PathType Leaf)) {
        return @{}
    }

    $rows = @(Import-Csv -LiteralPath $LiteralPath -Delimiter "`t")
    if ($rows.Count -ne $ExpectedCount) {
        throw "Frozen provenance lock expected $ExpectedCount rows but contains $($rows.Count)."
    }

    $lockedByUrl = @{}
    foreach ($row in $rows) {
        if ($lockedByUrl.ContainsKey($row.source_url)) {
            throw "Duplicate source URL in frozen provenance lock: $($row.source_url)"
        }
        $lockedByUrl[$row.source_url] = $row
    }

    return $lockedByUrl
}

function Get-ChecksumSidecarSha {
    param([string]$LiteralPath)

    $checksumText = (Get-Content -LiteralPath $LiteralPath -Raw).Trim()
    $checksumMatch = [regex]::Match($checksumText, '^(?<sha>[0-9a-fA-F]{64})\s+')
    if (-not $checksumMatch.Success) {
        throw "Malformed checksum metadata at $LiteralPath"
    }

    return $checksumMatch.Groups['sha'].Value.ToLowerInvariant()
}

function Resolve-ChecksumExpectation {
    param(
        [pscustomobject]$Month,
        [string]$ZipPath,
        [hashtable]$LockedRowsByUrl,
        [switch]$AllowFetch
    )

    $sidecarPath = "${ZipPath}.CHECKSUM"
    $lockedRow = $LockedRowsByUrl[$Month.source_url]

    if (Test-Path -LiteralPath $sidecarPath -PathType Leaf) {
        $officialSha = Get-ChecksumSidecarSha -LiteralPath $sidecarPath
        if ($null -ne $lockedRow) {
            $lockedSha = [string]$lockedRow.archive_sha256
            if (-not [string]::IsNullOrWhiteSpace($lockedSha) -and $officialSha -ne $lockedSha.ToLowerInvariant()) {
                throw "Checksum sidecar disagrees with frozen provenance lock for $($Month.source_url)."
            }
        }

        return [pscustomobject]@{
            sha256 = $officialSha
            source = 'sidecar'
            sidecar_path = $sidecarPath
        }
    }

    if ($null -ne $lockedRow) {
        $lockedSha = [string]$lockedRow.archive_sha256
        if ([string]::IsNullOrWhiteSpace($lockedSha)) {
            throw "Frozen provenance lock does not contain archive_sha256 for $($Month.source_url)."
        }

        return [pscustomobject]@{
            sha256 = $lockedSha.ToLowerInvariant()
            source = 'frozen_lock'
            sidecar_path = $null
        }
    }

    if (-not $AllowFetch) {
        throw "Missing checksum metadata for $($Month.zip_name). Add $($Month.zip_name).CHECKSUM, provide a frozen provenance lock, or rerun with -FetchMissingChecksums."
    }

    $response = Invoke-WebRequest -Uri $Month.checksum_url -UseBasicParsing
    $content = if ($response.Content -is [byte[]]) {
        [System.Text.Encoding]::UTF8.GetString($response.Content)
    }
    else {
        [string]$response.Content
    }
    [System.IO.File]::WriteAllText($sidecarPath, $content, [System.Text.UTF8Encoding]::new($false))

    return [pscustomobject]@{
        sha256 = (Get-ChecksumSidecarSha -LiteralPath $sidecarPath)
        source = 'downloaded_sidecar'
        sidecar_path = $sidecarPath
    }
}

function Read-ZipCsvBytes {
    param(
        [string]$ZipPath,
        [string]$ExpectedEntryName
    )

    $zip = [System.IO.Compression.ZipFile]::OpenRead($ZipPath)
    try {
        $entries = @($zip.Entries | Where-Object { -not [string]::IsNullOrEmpty($_.Name) })
        if ($entries.Count -ne 1 -or $entries[0].FullName -ne $ExpectedEntryName) {
            throw "Archive must contain exactly ${ExpectedEntryName}: $ZipPath"
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
            return $memory.ToArray()
        }
        finally {
            $memory.Dispose()
        }
    }
    finally {
        $zip.Dispose()
    }
}

function ConvertTo-NormalizedJsonValue {
    param($Value)

    if ($null -eq $Value) {
        return $null
    }

    if ($Value -is [System.Collections.IDictionary]) {
        $ordered = [ordered]@{}
        foreach ($key in @($Value.Keys | Sort-Object)) {
            $ordered[[string]$key] = ConvertTo-NormalizedJsonValue -Value $Value[$key]
        }
        return [pscustomobject]$ordered
    }

    if ($Value -is [string] -or $Value -is [ValueType]) {
        return $Value
    }

    if ($Value -is [System.Collections.IEnumerable]) {
        $items = [System.Collections.Generic.List[object]]::new()
        foreach ($item in $Value) {
            $items.Add((ConvertTo-NormalizedJsonValue -Value $item))
        }
        return @($items)
    }

    $properties = @($Value.PSObject.Properties)
    if ($properties.Count -eq 0) {
        return $Value
    }

    $ordered = [ordered]@{}
    foreach ($property in @($properties.Name | Sort-Object)) {
        $ordered[$property] = ConvertTo-NormalizedJsonValue -Value $Value.$property
    }
    return [pscustomobject]$ordered
}

function ConvertTo-CanonicalJson {
    param($Value)

    return ((ConvertTo-NormalizedJsonValue -Value $Value) | ConvertTo-Json -Depth 32)
}

function Test-ExpectedArtifactMatch {
    param(
        $ActualArtifact,
        [string]$ExpectedArtifactPath
    )

    $actualJson = ConvertTo-CanonicalJson -Value $ActualArtifact
    $expectedJson = ConvertTo-CanonicalJson -Value ((Get-Content -LiteralPath $ExpectedArtifactPath -Raw) | ConvertFrom-Json)
    if ($actualJson -ne $expectedJson) {
        throw "Observed audit JSON does not match expected artifact: $ExpectedArtifactPath"
    }
}

function Get-FirstMissingCanonicalHour {
    param(
        [System.Collections.Generic.HashSet[int64]]$AlignedOpenSet,
        [DateTimeOffset]$MonthStart,
        [DateTimeOffset]$NextMonth
    )

    for ($cursor = Get-UnixMicroseconds -Value $MonthStart; $cursor -lt (Get-UnixMicroseconds -Value $NextMonth); $cursor += $HourMicroseconds) {
        if (-not $AlignedOpenSet.Contains($cursor)) {
            return $cursor
        }
    }

    return $null
}

function Invoke-W1Audit {
    param(
        [string]$ResolvedCacheDirectory,
        [string]$ResolvedProvenanceLock,
        [switch]$AllowChecksumFetch,
        [string]$ResolvedProtocolId,
        [string]$ResolvedFrozenPreregistrationCommit,
        [string]$ResolvedSourceBaseUrl,
        [DateTimeOffset]$ResolvedFirstMonth,
        [DateTimeOffset]$ResolvedMonthAfterLast
    )

    $registry = Get-MonthRegistry -StartMonth $ResolvedFirstMonth `
        -ExclusiveEndMonth $ResolvedMonthAfterLast `
        -Symbol 'BTCUSDT' `
        -Interval '1h' `
        -BaseUrl $ResolvedSourceBaseUrl

    $lockedRowsByUrl = Get-LockedRowsByUrl -LiteralPath $ResolvedProvenanceLock -ExpectedCount $registry.Count
    $globalAlignedOpens = [System.Collections.Generic.HashSet[int64]]::new()

    $rawRows = 0
    $alignedRows = 0
    $offGridRows = 0
    $duplicateHourOpens = 0
    $timestampDiscontinuities = 0
    $monthsWithRowCountMismatch = 0
    $monthsFailingFullHourShape = 0
    $firstBlockingArchive = $null
    $previousOpenUs = $null

    foreach ($month in $registry) {
        $zipPath = Join-Path $ResolvedCacheDirectory $month.zip_name
        if (-not (Test-Path -LiteralPath $zipPath -PathType Leaf)) {
            throw "Missing archive cache file: $zipPath"
        }

        $checksumExpectation = Resolve-ChecksumExpectation -Month $month `
            -ZipPath $zipPath `
            -LockedRowsByUrl $lockedRowsByUrl `
            -AllowFetch:$AllowChecksumFetch

        $observedArchiveSha = Get-FileSha256Hex -LiteralPath $zipPath
        if ($observedArchiveSha -ne $checksumExpectation.sha256) {
            throw "Archive SHA-256 mismatch for $($month.source_url). Expected $($checksumExpectation.sha256) but observed $observedArchiveSha."
        }

        $csvBytes = Read-ZipCsvBytes -ZipPath $zipPath -ExpectedEntryName $month.csv_name
        $csvSha = Get-Sha256Hex -Bytes $csvBytes
        $csvText = [System.Text.Encoding]::UTF8.GetString($csvBytes)
        $lines = @($csvText -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($lines.Count -eq 0) {
            throw "Archive CSV is empty: $zipPath"
        }

        $firstFields = $lines[0].Split(',')
        $lastFields = $lines[-1].Split(',')
        if ($firstFields.Count -ne $ExpectedColumns -or $lastFields.Count -ne $ExpectedColumns) {
            throw "Archive endpoint rows do not have the $ExpectedColumns-column kline shape: $zipPath"
        }

        $timestampInfo = Resolve-BinanceTimestampUnit `
            -ObservedFirstOpen ([int64]$firstFields[0]) `
            -ObservedLastClose ([int64]$lastFields[6]) `
            -MonthStart $month.start `
            -NextMonth $month.next

        $hourWidthRaw = [int64]$timestampInfo.hour_width_raw
        $alignedMonthOpens = [System.Collections.Generic.HashSet[int64]]::new()
        $monthBadShapeCount = 0
        $monthBadCloseCount = 0
        $monthNonHourStepCount = 0
        $monthDuplicateOpenCount = 0
        $monthPreviousOpenUs = $null
        $monthFirstOpenRaw = $null
        $monthLastOpenRaw = $null
        $monthLastCloseRaw = $null

        for ($index = 0; $index -lt $lines.Count; $index++) {
            $fields = $lines[$index].Split(',')
            if ($fields.Count -ne $ExpectedColumns) {
                $monthBadShapeCount++
                continue
            }

            try {
                $openRaw = [int64]$fields[0]
                $closeRaw = [int64]$fields[6]
            }
            catch {
                $monthBadShapeCount++
                continue
            }

            if ($null -eq $monthFirstOpenRaw) {
                $monthFirstOpenRaw = $openRaw
            }
            $monthLastOpenRaw = $openRaw
            $monthLastCloseRaw = $closeRaw

            $openUs = Convert-RawTimestampToMicroseconds -RawValue $openRaw -TimestampUnit $timestampInfo.timestamp_unit

            if ($null -ne $previousOpenUs) {
                $deltaUs = $openUs - $previousOpenUs
                if ($deltaUs -ne $HourMicroseconds) {
                    $timestampDiscontinuities++
                }
            }
            $previousOpenUs = $openUs

            if ($null -ne $monthPreviousOpenUs) {
                $monthDeltaUs = $openUs - $monthPreviousOpenUs
                if ($monthDeltaUs -ne $HourMicroseconds) {
                    $monthNonHourStepCount++
                }
            }
            $monthPreviousOpenUs = $openUs

            if (($closeRaw - $openRaw) -ne ($hourWidthRaw - 1)) {
                $monthBadCloseCount++
            }

            if (($openUs % $HourMicroseconds) -eq 0) {
                $alignedRows++
                if (-not $alignedMonthOpens.Add($openUs)) {
                    $monthDuplicateOpenCount++
                }
                if (-not $globalAlignedOpens.Add($openUs)) {
                    $duplicateHourOpens++
                }
            }
            else {
                $offGridRows++
            }
        }

        $rawRows += $lines.Count
        if ($lines.Count -ne $month.expected_rows) {
            $monthsWithRowCountMismatch++
        }

        $monthStartRaw = if ($timestampInfo.timestamp_unit -eq 'milliseconds') {
            $month.start.ToUnixTimeMilliseconds()
        }
        else {
            Get-UnixMicroseconds -Value $month.start
        }
        $monthLastOpenExpectedRaw = if ($timestampInfo.timestamp_unit -eq 'milliseconds') {
            $month.next.AddHours(-1).ToUnixTimeMilliseconds()
        }
        else {
            Get-UnixMicroseconds -Value $month.next.AddHours(-1)
        }
        $monthEndRaw = $monthLastOpenExpectedRaw + $hourWidthRaw - 1

        $startsAtBoundary = $monthFirstOpenRaw -eq $monthStartRaw
        $lastOpenExpected = $monthLastOpenRaw -eq $monthLastOpenExpectedRaw
        $endsAtBoundary = $monthLastCloseRaw -eq $monthEndRaw
        $firstMissingCanonicalUs = Get-FirstMissingCanonicalHour -AlignedOpenSet $alignedMonthOpens -MonthStart $month.start -NextMonth $month.next

        $fullShapeFailure = ($lines.Count -ne $month.expected_rows) `
            -or ($monthBadShapeCount -gt 0) `
            -or ($monthBadCloseCount -gt 0) `
            -or ($monthDuplicateOpenCount -gt 0) `
            -or (-not $startsAtBoundary) `
            -or (-not $lastOpenExpected) `
            -or (-not $endsAtBoundary) `
            -or ($monthNonHourStepCount -gt 0)

        if ($fullShapeFailure) {
            $monthsFailingFullHourShape++
            if ($null -eq $firstBlockingArchive) {
                $firstBlockingArchive = [ordered]@{
                    source_url = $month.source_url
                    official_and_observed_archive_sha256 = $observedArchiveSha
                    csv_sha256 = $csvSha
                    expected_rows = $month.expected_rows
                    observed_rows = $lines.Count
                    missing_open_time_utc = if ($null -ne $firstMissingCanonicalUs) {
                        [DateTimeOffset]::FromUnixTimeMilliseconds([int64]($firstMissingCanonicalUs / 1000L)).ToString("yyyy-MM-ddTHH:mm:ss'Z'")
                    }
                    else {
                        $null
                    }
                }
            }
        }
    }

    $expectedCalendarHours = [int](($ResolvedMonthAfterLast - $ResolvedFirstMonth).TotalDays * 24)
    $missingCanonicalHourOpens = $expectedCalendarHours - $globalAlignedOpens.Count
    $hasBlockingFailure = ($offGridRows -gt 0) `
        -or ($missingCanonicalHourOpens -gt 0) `
        -or ($duplicateHourOpens -gt 0) `
        -or ($timestampDiscontinuities -gt 0) `
        -or ($monthsFailingFullHourShape -gt 0)

    $artifact = [ordered]@{
        schema_version = 1
        protocol_id = $ResolvedProtocolId
        frozen_preregistration_commit = $ResolvedFrozenPreregistrationCommit
        status = if ($hasBlockingFailure) { 'aborted_at_data_admission' } else { 'passed_data_admission' }
        source_base_url = $ResolvedSourceBaseUrl
        admission_contract = [ordered]@{
            expected_archive_count = $registry.Count
            expected_calendar_hours = $expectedCalendarHours
            required_interval_microseconds = $HourMicroseconds
            missing_or_off_grid_rows_allowed = 0
        }
        observed_structure = [ordered]@{
            official_archive_checksums_verified = $registry.Count
            raw_rows = $rawRows
            utc_aligned_rows = $alignedRows
            raw_row_deficit = $expectedCalendarHours - $rawRows
            off_grid_rows = $offGridRows
            missing_canonical_hour_opens = $missingCanonicalHourOpens
            duplicate_hour_opens = $duplicateHourOpens
            timestamp_discontinuities = $timestampDiscontinuities
            months_with_row_count_mismatch = $monthsWithRowCountMismatch
            months_failing_full_hour_shape = $monthsFailingFullHourShape
        }
        first_blocking_archive = $firstBlockingArchive
        experiment_state = [ordered]@{
            candidate_metrics_computed = $false
            selection_executed = $false
            selection_artifact_written = $false
            holdout_evaluation_opened = $false
            holdout_metrics_computed = $false
            holdout_structure_only_audited = $hasBlockingFailure
        }
        conclusion = if ($hasBlockingFailure) {
            'The frozen hourly experiment cannot be evaluated without changing its preregistered data and evaluation semantics. The Edge gate remains closed.'
        }
        else {
            'The frozen hourly dataset passed admission and may proceed to selection under the preregistered protocol.'
        }
        prohibited_followups_under_this_protocol_id = if ($hasBlockingFailure) {
            @(
                'insert synthetic flat bars',
                'shift or snap off-grid bars',
                'shorten or move the frozen windows',
                'silently replace failed 1h archives with 1m archives',
                'open selection or holdout evaluation'
            )
        }
        else {
            @()
        }
        separate_v2_requires_new_preregistration = $hasBlockingFailure
    }

    return [pscustomobject]$artifact
}

function New-MockMonthArchive {
    param(
        [string]$Directory,
        [string]$FileName,
        [string]$CsvName,
        [string[]]$Rows
    )

    $csvText = [string]::Join("`n", $Rows) + "`n"
    $csvBytes = [System.Text.Encoding]::UTF8.GetBytes($csvText)
    $zipPath = Join-Path $Directory $FileName

    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }

    $archive = [System.IO.Compression.ZipFile]::Open($zipPath, [System.IO.Compression.ZipArchiveMode]::Create)
    try {
        $entry = $archive.CreateEntry($CsvName)
        $stream = $entry.Open()
        try {
            $stream.Write($csvBytes, 0, $csvBytes.Length)
        }
        finally {
            $stream.Dispose()
        }
    }
    finally {
        $archive.Dispose()
    }

    $sha = Get-FileSha256Hex -LiteralPath $zipPath
    $sidecar = Join-Path $Directory "${FileName}.CHECKSUM"
    [System.IO.File]::WriteAllText($sidecar, "$sha  $FileName`n", [System.Text.UTF8Encoding]::new($false))
}

function Get-MockHourRow {
    param(
        [DateTimeOffset]$OpenUtc,
        [double]$BasePrice = 1000.0,
        [int]$Index = 0
    )

    $openUs = Get-UnixMicroseconds -Value $OpenUtc
    $closeUs = $openUs + $HourMicroseconds - 1
    $price = '{0:F2}' -f ($BasePrice + $Index)
    return "$openUs,$price,$price,$price,$price,1,$closeUs,1,1,1,1,0"
}

function Invoke-SelfTestRun {
    $selfTestRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("w1-audit-selftest-" + [guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $selfTestRoot -Force | Out-Null

    try {
        $monthOne = [DateTimeOffset]::Parse('2026-01-01T00:00:00Z')
        $monthTwo = [DateTimeOffset]::Parse('2026-02-01T00:00:00Z')
        $monthThree = [DateTimeOffset]::Parse('2026-03-01T00:00:00Z')

        $rowsOne = [System.Collections.Generic.List[string]]::new()
        $counter = 0
        for ($cursor = $monthOne; $cursor -lt $monthTwo; $cursor = $cursor.AddHours(1)) {
            $rowsOne.Add((Get-MockHourRow -OpenUtc $cursor -Index $counter))
            $counter++
        }
        New-MockMonthArchive -Directory $selfTestRoot `
            -FileName 'BTCUSDT-1h-2026-01.zip' `
            -CsvName 'BTCUSDT-1h-2026-01.csv' `
            -Rows @($rowsOne)

        $rowsTwo = [System.Collections.Generic.List[string]]::new()
        $counter = 0
        for ($cursor = $monthTwo; $cursor -lt $monthThree; $cursor = $cursor.AddHours(1)) {
            if ($cursor -eq [DateTimeOffset]::Parse('2026-02-10T05:00:00Z')) {
                continue
            }
            if ($cursor -eq [DateTimeOffset]::Parse('2026-02-20T07:00:00Z')) {
                $rowsTwo.Add((Get-MockHourRow -OpenUtc $cursor.AddMinutes(30) -BasePrice 2000.0 -Index $counter))
                $counter++
                continue
            }
            $rowsTwo.Add((Get-MockHourRow -OpenUtc $cursor -BasePrice 2000.0 -Index $counter))
            $counter++
        }
        New-MockMonthArchive -Directory $selfTestRoot `
            -FileName 'BTCUSDT-1h-2026-02.zip' `
            -CsvName 'BTCUSDT-1h-2026-02.csv' `
            -Rows @($rowsTwo)

        $artifact = Invoke-W1Audit `
            -ResolvedCacheDirectory $selfTestRoot `
            -ResolvedProvenanceLock (Join-Path $selfTestRoot 'missing.tsv') `
            -AllowChecksumFetch:$false `
            -ResolvedProtocolId 'selftest-w1-audit' `
            -ResolvedFrozenPreregistrationCommit 'selftest' `
            -ResolvedSourceBaseUrl 'https://fixture.invalid/monthly/klines/BTCUSDT/1h' `
            -ResolvedFirstMonth $monthOne `
            -ResolvedMonthAfterLast $monthThree

        if ($artifact.status -ne 'aborted_at_data_admission') {
            throw 'Self-test expected an aborted data admission result.'
        }
        if ($artifact.observed_structure.raw_rows -ne 1415) {
            throw "Self-test raw_rows mismatch: $($artifact.observed_structure.raw_rows)"
        }
        if ($artifact.observed_structure.utc_aligned_rows -ne 1414) {
            throw "Self-test utc_aligned_rows mismatch: $($artifact.observed_structure.utc_aligned_rows)"
        }
        if ($artifact.observed_structure.off_grid_rows -ne 1) {
            throw "Self-test off_grid_rows mismatch: $($artifact.observed_structure.off_grid_rows)"
        }
        if ($artifact.observed_structure.missing_canonical_hour_opens -ne 2) {
            throw "Self-test missing_canonical_hour_opens mismatch: $($artifact.observed_structure.missing_canonical_hour_opens)"
        }
        if ($artifact.observed_structure.months_with_row_count_mismatch -ne 1) {
            throw "Self-test months_with_row_count_mismatch mismatch: $($artifact.observed_structure.months_with_row_count_mismatch)"
        }
        if ($artifact.observed_structure.months_failing_full_hour_shape -ne 1) {
            throw "Self-test months_failing_full_hour_shape mismatch: $($artifact.observed_structure.months_failing_full_hour_shape)"
        }
        if ($artifact.first_blocking_archive.missing_open_time_utc -ne '2026-02-10T05:00:00Z') {
            throw "Self-test first_blocking_archive mismatch: $($artifact.first_blocking_archive.missing_open_time_utc)"
        }

        return [pscustomobject]@{
            self_test = 'passed'
            fixture_cache = $selfTestRoot
            raw_rows = $artifact.observed_structure.raw_rows
            missing_canonical_hour_opens = $artifact.observed_structure.missing_canonical_hour_opens
        }
    }
    finally {
        if (Test-Path -LiteralPath $selfTestRoot) {
            Remove-Item -LiteralPath $selfTestRoot -Recurse -Force
        }
    }
}

if ($SelfTest) {
    Invoke-SelfTestRun
    return
}

$resolvedCacheDirectory = [System.IO.Path]::GetFullPath($CacheDirectory)
$defaultProvenanceLock = Join-Path (Split-Path -Parent $PSCommandPath) '..\artifacts\strategy-evaluation\w1-1h\w1-btcusdt-1h-provenance.tsv'
if ([string]::IsNullOrWhiteSpace($ProvenanceLock)) {
    $ProvenanceLock = $defaultProvenanceLock
}
$resolvedProvenanceLock = [System.IO.Path]::GetFullPath($ProvenanceLock)
New-Item -ItemType Directory -Path $resolvedCacheDirectory -Force | Out-Null

$artifact = Invoke-W1Audit `
    -ResolvedCacheDirectory $resolvedCacheDirectory `
    -ResolvedProvenanceLock $resolvedProvenanceLock `
    -AllowChecksumFetch:$FetchMissingChecksums `
    -ResolvedProtocolId $ProtocolId `
    -ResolvedFrozenPreregistrationCommit $FrozenPreregistrationCommit `
    -ResolvedSourceBaseUrl $SourceBaseUrl `
    -ResolvedFirstMonth $FirstMonth `
    -ResolvedMonthAfterLast $MonthAfterLast

if (-not [string]::IsNullOrWhiteSpace($OutputPath)) {
    $resolvedOutputPath = [System.IO.Path]::GetFullPath($OutputPath)
    New-Item -ItemType Directory -Path (Split-Path -Parent $resolvedOutputPath) -Force | Out-Null
    [System.IO.File]::WriteAllText($resolvedOutputPath, ($artifact | ConvertTo-Json -Depth 16), [System.Text.UTF8Encoding]::new($false))
}

if (-not [string]::IsNullOrWhiteSpace($ExpectedArtifact)) {
    Test-ExpectedArtifactMatch -ActualArtifact $artifact -ExpectedArtifactPath ([System.IO.Path]::GetFullPath($ExpectedArtifact))
}

$artifact
