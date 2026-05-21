# scripts/install_gpu.ps1
# BrainFlow GPU/Cellpose optional dependency installer - Windows PowerShell
# Usage from project root: .\scripts\install_gpu.ps1
# Usage from scripts dir: .\install_gpu.ps1
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvDir = Join-Path $ProjectRoot ".venv"
$BackendDir = Join-Path $ProjectRoot "backend"
$ReqFile = Join-Path $BackendDir "requirements-gpu.txt"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " BrainFlow GPU Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Project root: $ProjectRoot" -ForegroundColor Gray
Write-Host ""

if (-not (Test-Path $PythonExe)) {
    throw "Python venv not found at $VenvDir. Run .\scripts\setup.ps1 first."
}

if (-not (Test-Path $ReqFile)) {
    throw "requirements-gpu.txt not found at $ReqFile"
}

Write-Host "Installing GPU/Cellpose dependencies..." -ForegroundColor Yellow
& $PythonExe -m pip install -r $ReqFile
Write-Host ""
Write-Host "GPU deps installed." -ForegroundColor Green
Write-Host "Restart the backend if it is running." -ForegroundColor Cyan
