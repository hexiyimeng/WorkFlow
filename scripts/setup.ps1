# scripts/setup.ps1
# BrainFlow basic setup script - Windows PowerShell
# Usage from project root: .\scripts\setup.ps1
# Usage from scripts dir: .\setup.ps1
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvDir = Join-Path $ProjectRoot ".venv"
$FrontendDir = Join-Path $ProjectRoot "frontend"
$BackendDir = Join-Path $ProjectRoot "backend"
$ReqFile = Join-Path $BackendDir "requirements.txt"
$BuildScript = Join-Path $ProjectRoot "scripts\build_frontend.ps1"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " BrainFlow Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Project root: $ProjectRoot" -ForegroundColor Gray
Write-Host ""

# 1. Python venv
Write-Host "[1/5] Setting up Python virtual environment..." -ForegroundColor Yellow
if (Test-Path $PythonExe) {
    Write-Host "  .venv already exists, reusing: $VenvDir"
} else {
    if (Test-Path $VenvDir) {
        Write-Warning "  .venv exists but python.exe is missing. Removing broken venv..."
        Remove-Item -Recurse -Force $VenvDir
    }
    Write-Host "  Creating .venv at $VenvDir ..."
    python -m venv $VenvDir
}

if (-not (Test-Path $PythonExe)) {
    throw "Python executable was not created at $PythonExe"
}

# Ensure pip exists
Write-Host "[2/5] Ensuring pip is available and up to date..." -ForegroundColor Yellow
& $PythonExe -m pip --version *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Warning "  pip is missing in .venv. Running ensurepip..."
    & $PythonExe -m ensurepip --upgrade
}
& $PythonExe -m pip install --upgrade pip

# 3. Install backend requirements
Write-Host "[3/5] Installing backend dependencies..." -ForegroundColor Yellow
if (Test-Path $ReqFile) {
    & $PythonExe -m pip install -r $ReqFile
    Write-Host "  Backend deps installed." -ForegroundColor Green
} else {
    Write-Warning "  requirements.txt not found at $ReqFile, skipping."
}

# 4. Install frontend deps
Write-Host "[4/5] Installing frontend dependencies..." -ForegroundColor Yellow
if (Test-Path $FrontendDir) {
    Push-Location $FrontendDir
    try {
        $LockFile = Join-Path $FrontendDir "package-lock.json"
        if (Test-Path $LockFile) {
            Write-Host "  package-lock.json found. Running npm ci..."
            npm ci
        } else {
            Write-Host "  package-lock.json not found. Running npm install..."
            npm install
        }
        Write-Host "  Frontend deps installed." -ForegroundColor Green
    } finally {
        Pop-Location
    }
} else {
    Write-Warning "  frontend/ not found at $FrontendDir, skipping."
}

# 5. Build frontend
Write-Host "[5/5] Building frontend..." -ForegroundColor Yellow
if (Test-Path $BuildScript) {
    & $BuildScript
} else {
    Write-Warning "  build_frontend.ps1 not found at $BuildScript, skipping build."
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
