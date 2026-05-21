# scripts/build_frontend.ps1
# BrainFlow frontend build script - Windows PowerShell
# Usage from project root: .\scripts\build_frontend.ps1
# Usage from scripts dir: .\build_frontend.ps1
$ErrorActionPreference = "Stop"

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
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
    npm run build
} finally {
    Pop-Location
}

# Verify output
$IndexHtml = Join-Path $DistDir "index.html"
if (-not (Test-Path $IndexHtml)) {
    throw "Build finished, but backend/dist/index.html was not found at $IndexHtml. Check frontend/vite.config.ts build.outDir."
}

Write-Host ""
Write-Host "Frontend built successfully." -ForegroundColor Green
Write-Host "Output: $IndexHtml" -ForegroundColor Cyan
