# scripts/update.ps1
# WorkFlow update script - pull latest code and rebuild - Windows PowerShell
# Usage from project root: .\scripts\update.ps1
# Usage from scripts dir: .\update.ps1
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

$VenvDir = Join-Path $ProjectRoot ".venv"
$BackendDir = Join-Path $ProjectRoot "backend"
$FrontendDir = Join-Path $ProjectRoot "frontend"
$ReqFile = Join-Path $BackendDir "requirements.txt"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
$BuildScript = Join-Path $ProjectRoot "scripts\build_frontend.ps1"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " WorkFlow Update" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Project root: $ProjectRoot" -ForegroundColor Gray
Write-Host ""

Write-Host "[1/4] Pulling latest code..." -ForegroundColor Yellow
Set-Location $ProjectRoot
Invoke-Step -Command { git pull } -ErrorMessage "git pull failed."

if (-not (Test-Path $PythonExe)) {
    throw "Python venv not found at $VenvDir. Run .\scripts\setup.ps1 first."
}

$PipArgs = Get-PipInstallArgs

Write-Host "[2/4] Updating backend dependencies..." -ForegroundColor Yellow
if (Test-Path $ReqFile) {
    Invoke-Step -Command { & $PythonExe -m pip install -r $ReqFile --upgrade @PipArgs } -ErrorMessage "Backend dependency update failed."
    Write-Host "  Backend deps updated." -ForegroundColor Green
} else {
    throw "requirements.txt not found at $ReqFile"
}

Write-Host "[3/4] Updating frontend dependencies..." -ForegroundColor Yellow
if (Test-Path $FrontendDir) {
    Push-Location $FrontendDir
    try {
        $LockFile = Join-Path $FrontendDir "package-lock.json"
        if (Test-Path $LockFile) {
            Invoke-Step -Command { npm ci } -ErrorMessage "npm ci failed."
        } else {
            Invoke-Step -Command { npm install } -ErrorMessage "npm install failed."
        }
        Write-Host "  Frontend deps updated." -ForegroundColor Green
    } finally {
        Pop-Location
    }
} else {
    throw "frontend/ not found at $FrontendDir"
}

Write-Host "[4/4] Rebuilding frontend..." -ForegroundColor Yellow
if (Test-Path $BuildScript) {
    & $BuildScript
    if ($LASTEXITCODE -ne 0) {
        throw "Frontend rebuild failed."
    }
} else {
    throw "build_frontend.ps1 not found at $BuildScript"
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Update complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Please restart the backend:" -ForegroundColor Cyan
Write-Host "  .\scripts\start.ps1" -ForegroundColor Gray
Write-Host ""
