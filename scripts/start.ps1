# scripts/start.ps1
# WorkFlow backend startup script - Windows PowerShell
# Usage from project root: .\scripts\start.ps1
# Usage from scripts dir: .\start.ps1
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
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
$MainPy = Join-Path $BackendDir "main.py"
$DistIndex = Join-Path $BackendDir "dist\index.html"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " WorkFlow Backend" -ForegroundColor Cyan
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

Write-Host "Checking backend dependency import..." -ForegroundColor Yellow
Invoke-Step -Command { & $PythonExe -c "import fastapi, uvicorn, dask, distributed, zarr; print('  backend deps ok')" } -ErrorMessage "Backend imports failed. Run .\scripts\setup.ps1 again."

Set-Location $BackendDir
Write-Host "Starting backend on http://localhost:8000" -ForegroundColor Green
Write-Host "Press Ctrl+C to stop." -ForegroundColor Gray
Write-Host ""

& $PythonExe $MainPy
