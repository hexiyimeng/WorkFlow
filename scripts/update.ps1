# scripts/update.ps1
# BrainFlow update script - pull latest code and rebuild - Windows PowerShell
# Usage from project root: .\scripts\update.ps1
# Usage from scripts dir: .\update.ps1
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvDir = Join-Path $ProjectRoot ".venv"
$BackendDir = Join-Path $ProjectRoot "backend"
$FrontendDir = Join-Path $ProjectRoot "frontend"
$ReqFile = Join-Path $BackendDir "requirements.txt"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
$BuildScript = Join-Path $ProjectRoot "scripts\build_frontend.ps1"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " BrainFlow Update" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Project root: $ProjectRoot" -ForegroundColor Gray
Write-Host ""

# 1. Git pull
Write-Host "[1/4] Pulling latest code..." -ForegroundColor Yellow
Set-Location $ProjectRoot
git pull
if ($LASTEXITCODE -ne 0) { throw "git pull failed." }

if (-not (Test-Path $PythonExe)) {
    throw "Python venv not found at $VenvDir. Run .\scripts\setup.ps1 first."
}

# 2. Update backend deps
Write-Host "[2/4] Updating backend dependencies..." -ForegroundColor Yellow
if (Test-Path $ReqFile) {
    & $PythonExe -m pip install -r $ReqFile --upgrade
    Write-Host "  Backend deps updated." -ForegroundColor Green
} else {
    Write-Warning "  requirements.txt not found at $ReqFile, skipping."
}

# 3. Update frontend deps
Write-Host "[3/4] Updating frontend dependencies..." -ForegroundColor Yellow
if (Test-Path $FrontendDir) {
    Push-Location $FrontendDir
    try {
        $LockFile = Join-Path $FrontendDir "package-lock.json"
        if (Test-Path $LockFile) {
            npm ci
        } else {
            npm install
        }
        Write-Host "  Frontend deps updated." -ForegroundColor Green
    } finally {
        Pop-Location
    }
} else {
    Write-Warning "  frontend/ not found at $FrontendDir, skipping."
}

# 4. Rebuild frontend
Write-Host "[4/4] Rebuilding frontend..." -ForegroundColor Yellow
if (Test-Path $BuildScript) {
    & $BuildScript
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
