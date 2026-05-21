# scripts/start.ps1
# BrainFlow backend startup script - Windows PowerShell
# Usage from project root: .\scripts\start.ps1
# Usage from scripts dir: .\start.ps1
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvDir = Join-Path $ProjectRoot ".venv"
$BackendDir = Join-Path $ProjectRoot "backend"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
$MainPy = Join-Path $BackendDir "main.py"
$DistIndex = Join-Path $BackendDir "dist\index.html"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " BrainFlow Backend" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Project root: $ProjectRoot" -ForegroundColor Gray
Write-Host ""

if (-not (Test-Path $PythonExe)) {
    throw "Python not found at $PythonExe. Run .\scripts\setup.ps1 first."
}

if (-not (Test-Path $MainPy)) {
    throw "backend/main.py not found at $MainPy"
}

if (-not (Test-Path $DistIndex)) {
    Write-Warning "backend/dist/index.html not found. The backend can start, but the frontend UI may not be available. Run .\scripts\build_frontend.ps1 first."
}

Set-Location $BackendDir
Write-Host "Starting backend on http://localhost:8000" -ForegroundColor Green
Write-Host "Press Ctrl+C to stop." -ForegroundColor Gray
Write-Host ""

& $PythonExe $MainPy
