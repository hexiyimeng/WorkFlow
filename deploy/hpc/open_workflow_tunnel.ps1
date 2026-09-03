param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9._-]+$')]
    [string]$User,

    [ValidatePattern('^[A-Za-z0-9.-]+$')]
    [string]$ClusterHost = '10.200.201.2',

    [ValidateRange(1, 65535)]
    [int]$LocalPort = 18000,

    [ValidateRange(1, 65535)]
    [int]$RemotePort = 8000,

    [ValidateRange(1, 65535)]
    [int]$DashboardLocalPort = 18787,

    [ValidateRange(1, 65535)]
    [int]$DashboardRemotePort = 8787
)

$ssh = (Get-Command ssh.exe -ErrorAction Stop).Source
$forward = "127.0.0.1:${LocalPort}:127.0.0.1:${RemotePort}"
$dashboardForward = "127.0.0.1:${DashboardLocalPort}:127.0.0.1:${DashboardRemotePort}"
$target = "${User}@${ClusterHost}"
$logDirectory = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'WorkFlow'
$logPath = Join-Path $logDirectory 'ssh-tunnel.log'
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
$arguments = @(
    '-N',
    '-T',
    '-v',
    '-E', $logPath,
    '-o', 'ExitOnForwardFailure=yes',
    '-o', 'ServerAliveInterval=30',
    '-o', 'ServerAliveCountMax=3',
    '-o', 'TCPKeepAlive=yes',
    '-o', 'ConnectTimeout=20',
    '-L', $forward,
    '-L', $dashboardForward,
    $target
)

Write-Host "A separate SSH window will request your existing cluster password."
Write-Host "No password or private key is read or stored by this script."
$process = Start-Process -FilePath $ssh -ArgumentList $arguments -PassThru

$deadline = [DateTime]::UtcNow.AddSeconds(60)
$connected = $false
while ([DateTime]::UtcNow -lt $deadline) {
    if ($process.HasExited) {
        throw "SSH tunnel exited with code $($process.ExitCode)."
    }
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $pending = $client.ConnectAsync('127.0.0.1', $LocalPort)
        if ($pending.Wait(500) -and $client.Connected) {
            $connected = $true
            break
        }
    }
    catch {
        # The local listener appears only after SSH authentication succeeds.
    }
    finally {
        $client.Dispose()
    }
    Start-Sleep -Milliseconds 500
}

if (-not $connected) {
    if (-not $process.HasExited) {
        $process.Kill()
    }
    throw 'SSH authentication or local port forwarding did not complete within 60 seconds.'
}

$url = "http://127.0.0.1:${LocalPort}/"
Write-Host "WorkFlow tunnel is ready: $url"
Write-Host "Dask dashboard tunnel: http://127.0.0.1:${DashboardLocalPort}/status"
Write-Host "SSH diagnostics: $logPath"
Write-Host 'Keep the SSH window open while using WorkFlow.'
Start-Process $url

# Keep this helper (and therefore the console hosting the interactive SSH
# process) alive for the lifetime of the tunnel. Closing this helper window or
# pressing Ctrl+C ends the exact child tunnel instead of leaving it detached.
try {
    Wait-Process -Id $process.Id
    $process.Refresh()
    Write-Warning "SSH tunnel exited with code $($process.ExitCode). Diagnostics: $logPath"
}
finally {
    if (-not $process.HasExited) {
        Stop-Process -Id $process.Id
    }
}
