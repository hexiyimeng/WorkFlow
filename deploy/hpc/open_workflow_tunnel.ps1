param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9._-]+$')]
    [string]$User,

    [ValidatePattern('^[A-Za-z0-9.-]+$')]
    [string]$ClusterHost = '10.200.201.2',

    [ValidateRange(1, 65535)]
    [int]$LocalPort = 18000,

    [ValidateRange(1, 65535)]
    [int]$RemotePort = 8000
)

$ssh = (Get-Command ssh.exe -ErrorAction Stop).Source
$forward = "127.0.0.1:${LocalPort}:127.0.0.1:${RemotePort}"
$target = "${User}@${ClusterHost}"
$arguments = @(
    '-N',
    '-T',
    '-o', 'ExitOnForwardFailure=yes',
    '-o', 'ServerAliveInterval=30',
    '-o', 'ServerAliveCountMax=3',
    '-L', $forward,
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
Write-Host 'Keep the SSH window open while using WorkFlow.'
Start-Process $url
