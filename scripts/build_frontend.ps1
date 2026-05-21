# scripts/build_frontend.ps1
# WorkFlow frontend build script - Windows PowerShell
# Usage from project root: .\scripts\build_frontend.ps1
# Usage from scripts dir: .\build_frontend.ps1
# This script is generated for WorkFlow deployment.
# It assumes this file lives in: <project-root>\scripts\
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Invoke-Step {
    param(
        [Parameter(Mandatory=$true)]
        [ScriptBlock]$Command,
        [Parameter(Mandatory=$true)]
        [string]$ErrorMessage
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw $ErrorMessage
    }
}

function Get-PipInstallArgs {
    $args = @()
    if ($env:WORKFLOW_PIP_INDEX_URL) {
        $args += @("-i", $env:WORKFLOW_PIP_INDEX_URL)
    } else {
        $args += @("-i", "https://pypi.tuna.tsinghua.edu.cn/simple")
    }

    if ($env:WORKFLOW_PIP_TRUSTED_HOST) {
        $args += @("--trusted-host", $env:WORKFLOW_PIP_TRUSTED_HOST)
    } else {
        $args += @("--trusted-host", "pypi.tuna.tsinghua.edu.cn")
    }
    return $args
}

$FrontendDir = Join-Path $ProjectRoot "frontend"
$DistDir = Join-Path $ProjectRoot "backend\dist"

Write-Host ""
Write-Host "[Frontend] Building..." -ForegroundColor Yellow
Write-Host "Project root: $ProjectRoot" -ForegroundColor Gray

if (-not (Test-Path $FrontendDir)) {
    throw "frontend/ directory not found at $FrontendDir"
}

Push-Location $FrontendDir
try {
    Invoke-Step -Command { npm run build } -ErrorMessage "npm run build failed."
} finally {
    Pop-Location
}

$IndexHtml = Join-Path $DistDir "index.html"
if (-not (Test-Path $IndexHtml)) {
    throw "Build finished, but backend/dist/index.html was not found at $IndexHtml. Check frontend/vite.config.ts build.outDir."
}

Write-Host ""
Write-Host "Frontend built successfully." -ForegroundColor Green
Write-Host "Output: $IndexHtml" -ForegroundColor Cyan
