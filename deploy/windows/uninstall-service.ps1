param([string] $ServiceName = "RaftCore")
$ErrorActionPreference = "Stop"
if (Get-Service $ServiceName -ErrorAction SilentlyContinue) {
  Stop-Service $ServiceName -ErrorAction SilentlyContinue
  sc.exe delete $ServiceName | Out-Null
  Write-Host "Servizio $ServiceName rimosso. I dati (cartella data) NON sono stati eliminati."
}
