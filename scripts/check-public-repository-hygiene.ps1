$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$violations = [System.Collections.Generic.List[string]]::new()

Push-Location $repositoryRoot
try {
    foreach ($requiredPath in @(
        '.env.example',
        '.gitleaksignore',
        '.github/workflows/secret-scan.yml',
        'archive/README.md',
        'docs/README.md'
    )) {
        if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
            $violations.Add("missing required public-repository file: $requiredPath")
        }
    }

    $absolutePaths = @(& git grep -n -I -E 'C:[\\/]+Users[\\/]' -- .)
    if ($LASTEXITCODE -notin @(0, 1)) {
        throw "git grep failed while checking local absolute paths"
    }
    foreach ($match in $absolutePaths) {
        $violations.Add("local absolute path is public: $match")
    }

    $volumeCharacter = [char]0x91CF
    $manipulationTermsPattern = '{0}{1}|{2}{1}' -f [char]0x5237, $volumeCharacter, [char]0x505A
    $manipulationTerms = @(& git grep -n -I -E $manipulationTermsPattern -- README.md rust/README.md rust/config frontend/src)
    if ($LASTEXITCODE -notin @(0, 1)) {
        throw "git grep failed while checking active product language"
    }
    foreach ($match in $manipulationTerms) {
        $violations.Add("active product language implies artificial volume: $match")
    }

    $marketManipulationChinese = -join ([char[]]@(0x5E02, 0x573A, 0x64CD, 0x7EB5))
    if (-not (Select-String -LiteralPath README.md -Quiet -SimpleMatch $marketManipulationChinese)) {
        $violations.Add('README.md must prohibit market manipulation in Chinese')
    }
    if (-not (Select-String -LiteralPath README.md -Quiet -SimpleMatch 'market manipulation')) {
        $violations.Add('README.md must prohibit market manipulation in English')
    }
} finally {
    Pop-Location
}

if ($violations.Count -gt 0) {
    throw ($violations -join [Environment]::NewLine)
}

Write-Output 'public repository hygiene checks passed'
exit 0
