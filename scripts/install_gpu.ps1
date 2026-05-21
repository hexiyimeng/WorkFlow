# scripts/install_gpu.ps1
# WorkFlow GPU/Cellpose optional dependency installer - Windows PowerShell
# Usage from project root: .\scripts\install_gpu.ps1
# Usage from scripts dir: .\install_gpu.ps1
#
# Optional examples:
#   .\scripts\install_gpu.ps1 -TorchCuda cu121
#   .\scripts\install_gpu.ps1 -TorchCuda cu126
#   .\scripts\install_gpu.ps1 -SkipTorchInstall
#
# A physical NVIDIA GPU is not enough: torch.cuda.is_available() must be True.

param(
    [string]$TorchCuda = $(if ($env:WORKFLOW_TORCH_CUDA) { $env:WORKFLOW_TORCH_CUDA } else { "cu121" }),
    [switch]$SkipTorchInstall
)

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
$ReqFile = Join-Path $BackendDir "requirements-gpu.txt"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " WorkFlow GPU Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Project root: $ProjectRoot" -ForegroundColor Gray
Write-Host ""

if (-not (Test-Path $PythonExe)) {
    throw "Python venv not found at $VenvDir. Run .\scripts\setup.ps1 first."
}

Write-Host "[GPU] Checking NVIDIA driver..." -ForegroundColor Yellow
$nvidiaSmi = Get-Command nvidia-smi -ErrorAction SilentlyContinue
if ($null -eq $nvidiaSmi) {
    Write-Warning "nvidia-smi was not found. If this machine has an NVIDIA GPU, install/update the NVIDIA driver first."
} else {
    & nvidia-smi
}

if (-not $SkipTorchInstall) {
    $TorchIndex = "https://download.pytorch.org/whl/$TorchCuda"
    Write-Host ""
    Write-Host "[GPU] Installing CUDA-enabled PyTorch from $TorchIndex ..." -ForegroundColor Yellow
    Write-Host "      Use -TorchCuda cu126/cu128/etc if your driver requires a different build." -ForegroundColor Gray

    Invoke-Step -Command {
        & $PythonExe -m pip install --upgrade --force-reinstall torch torchvision torchaudio --index-url $TorchIndex
    } -ErrorMessage "CUDA PyTorch installation failed. Try another -TorchCuda value or check network/driver."
} else {
    Write-Host "[GPU] Skipping PyTorch install because -SkipTorchInstall was provided." -ForegroundColor Yellow
}

if (Test-Path $ReqFile) {
    Write-Host ""
    Write-Host "[GPU] Installing Cellpose/GPU requirements..." -ForegroundColor Yellow
    $PipArgs = Get-PipInstallArgs
    Invoke-Step -Command {
        & $PythonExe -m pip install -r $ReqFile @PipArgs
    } -ErrorMessage "GPU/Cellpose dependency installation failed."
} else {
    throw "requirements-gpu.txt not found at $ReqFile"
}

Write-Host ""
Write-Host "[GPU] Verifying torch CUDA availability..." -ForegroundColor Yellow
& $PythonExe -c "import torch; print('torch:', torch.__version__); print('cuda available:', torch.cuda.is_available()); print('cuda version:', torch.version.cuda); print('gpu count:', torch.cuda.device_count()); print('gpu name:', torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'N/A')"
if ($LASTEXITCODE -ne 0) {
    throw "Torch CUDA verification failed."
}

Write-Host ""
Write-Host "GPU deps installed. Restart the backend if it is running." -ForegroundColor Green
Write-Host "If cuda available is False, the backend will still run in CPU mode." -ForegroundColor Yellow
