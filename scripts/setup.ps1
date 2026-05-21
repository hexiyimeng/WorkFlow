# scripts/setup.ps1
# WorkFlow basic setup script - Windows PowerShell
# Usage from project root: .\scripts\setup.ps1
# Usage from scripts dir: .\setup.ps1
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
$FrontendDir = Join-Path $ProjectRoot "frontend"
$BackendDir = Join-Path $ProjectRoot "backend"
$ReqFile = Join-Path $BackendDir "requirements.txt"
$BuildScript = Join-Path $ProjectRoot "scripts\build_frontend.ps1"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " WorkFlow Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Project root: $ProjectRoot" -ForegroundColor Gray
Write-Host ""

Write-Host "[1/5] Setting up Python virtual environment..." -ForegroundColor Yellow
if (Test-Path $PythonExe) {
    Write-Host "  .venv already exists, reusing: $VenvDir"
} else {
    if (Test-Path $VenvDir) {
        Write-Warning "  .venv exists but python.exe is missing. Removing broken venv..."
        Remove-Item -Recurse -Force $VenvDir
    }

    Write-Host "  Creating .venv at $VenvDir ..."
    Invoke-Step -Command { python -m venv $VenvDir } -ErrorMessage "Failed to create Python virtual environment."
}

if (-not (Test-Path $PythonExe)) {
    throw "Python executable was not created at $PythonExe"
}

Write-Host "[2/5] Ensuring pip is available and up to date..." -ForegroundColor Yellow
& $PythonExe -m pip --version *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Warning "  pip is missing in .venv. Running ensurepip..."
    Invoke-Step -Command { & $PythonExe -m ensurepip --upgrade } -ErrorMessage "ensurepip failed. Your Python installation may be broken."
}

$PipArgs = Get-PipInstallArgs
Invoke-Step -Command { & $PythonExe -m pip install --upgrade pip @PipArgs } -ErrorMessage "Failed to upgrade pip."

Write-Host "[3/5] Installing backend dependencies..." -ForegroundColor Yellow
if (Test-Path $ReqFile) {
    Invoke-Step -Command { & $PythonExe -m pip install -r $ReqFile @PipArgs } -ErrorMessage "Backend dependency installation failed."
    Write-Host "  Backend deps installed." -ForegroundColor Green
} else {
    throw "requirements.txt not found at $ReqFile"
}

Write-Host "[4/5] Installing frontend dependencies..." -ForegroundColor Yellow
if (Test-Path $FrontendDir) {
    Push-Location $FrontendDir
    try {
        $LockFile = Join-Path $FrontendDir "package-lock.json"
        if (Test-Path $LockFile) {
            Write-Host "  package-lock.json found. Running npm ci..."
            Invoke-Step -Command { npm ci } -ErrorMessage "npm ci failed."
        } else {
            Write-Host "  package-lock.json not found. Running npm install..."
            Invoke-Step -Command { npm install } -ErrorMessage "npm install failed."
        }
        Write-Host "  Frontend deps installed." -ForegroundColor Green
    } finally {
        Pop-Location
    }
} else {
    throw "frontend/ not found at $FrontendDir"
}

Write-Host "[5/5] Building frontend..." -ForegroundColor Yellow
if (Test-Path $BuildScript) {
    & $BuildScript
    if ($LASTEXITCODE -ne 0) {
        throw "Frontend build failed."
    }
} else {
    throw "build_frontend.ps1 not found at $BuildScript"
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Setup complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  .\scripts\start.ps1          # Start backend"
Write-Host "  .\scripts\install_gpu.ps1    # Optional: install GPU/Cellpose deps"
Write-Host ""
