<#
.SYNOPSIS
  Installa RaftCore come Windows Service.
.EXAMPLE
  .\install-service.ps1 -InstallDir C:\RaftCore -NodeId 11111111-1111-1111-1111-111111111111 `
     -ClusterSecret (Read-Host -AsSecureString) -SqlConnection "Server=sql01;Database=Ingest;Integrated Security=True;TrustServerCertificate=True"
#>
param(
  [Parameter(Mandatory)] [string] $InstallDir,
  [Parameter(Mandatory)] [string] $NodeId,
  [Parameter(Mandatory)] [SecureString] $ClusterSecret,
  [string] $SqlConnection = "",
  [string] $ServiceName = "RaftCore",
  [switch] $FirstStart
)
$ErrorActionPreference = "Stop"
$exe = Join-Path $InstallDir "RaftCore.exe"
if (-not (Test-Path $exe)) { throw "RaftCore.exe non trovato in $InstallDir" }

New-Item -ItemType Directory -Force -Path (Join-Path $InstallDir "data") | Out-Null
New-Service -Name $ServiceName -BinaryPathName "`"$exe`"" -DisplayName "RaftCore (nodo $NodeId)" -StartupType Automatic `
  -Description "Nodo del cluster RaftCore ad alta affidabilità" | Out-Null

# Variabili d'ambiente del solo servizio (non visibili agli altri processi)
$plain = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($ClusterSecret))
$envVars = @("CLUSTER_SECRET=$plain", "RaftCluster__NodeId=$NodeId", "RaftCluster__Storage__DataDir=$(Join-Path $InstallDir 'data')")
if ($SqlConnection) { $envVars += "RAFT_SQL_CONNECTION=$SqlConnection" }
if ($FirstStart) { $envVars += "RaftCluster__Storage__AllowEmptyState=true" }
Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\$ServiceName" -Name Environment -Type MultiString -Value $envVars

# Riavvio automatico in caso di errore (5 s, 5 s, 30 s)
sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000/restart/30000 | Out-Null
# Regole firewall: REST tra i nodi e discovery UDP
New-NetFirewallRule -DisplayName "RaftCore REST" -Direction Inbound -Protocol TCP -LocalPort 5000 -Action Allow -ErrorAction SilentlyContinue | Out-Null
New-NetFirewallRule -DisplayName "RaftCore discovery" -Direction Inbound -Protocol UDP -LocalPort 8888 -Action Allow -ErrorAction SilentlyContinue | Out-Null

Write-Host "Servizio $ServiceName installato. Avvio: Start-Service $ServiceName"
if ($FirstStart) { Write-Host "Dopo il primo avvio riuscito rieseguire senza -FirstStart (AllowEmptyState va rimosso)." }
