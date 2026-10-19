# Installazione come Windows Service

1. Scaricare dalla [pagina delle release](https://github.com/emanuelemerlo/raft-core/releases) `raftcore-<versione>-win-x64.zip`. È un eseguibile self-contained: .NET non va installato.
2. Estrarlo, ad esempio in `C:\RaftCore`, e configurare `C:\RaftCore\appsettings.json` (`ClusterId`, `ClusterSize`, `Members`, `Peers`, `StateMachine`). Un esempio è in `deploy\config\appsettings.Production.example.json`.
3. Da PowerShell **come amministratore**, sul **primo avvio** di ciascun nodo:

```powershell
cd C:\RaftCore\deploy\windows
.\install-service.ps1 -InstallDir C:\RaftCore -NodeId 11111111-1111-1111-1111-111111111111 `
   -ClusterSecret (Read-Host "Segreto del cluster" -AsSecureString) `
   -SqlConnection "Server=sql01;Database=Ingest;Integrated Security=True;TrustServerCertificate=True" -FirstStart
Start-Service RaftCore
```

4. Quando il nodo risulta avviato (`Invoke-RestMethod http://localhost:5000/health/live`), si rimuove il permesso di primo avvio rieseguendo lo script **senza** `-FirstStart` e riavviando il servizio. In alternativa si toglie la riga `RaftCluster__Storage__AllowEmptyState=true` dalla chiave di registro `HKLM\SYSTEM\CurrentControlSet\Services\RaftCore\Environment`.

Lo script:
- crea il servizio con avvio automatico e riavvio in caso di errore;
- imposta le variabili d'ambiente del solo servizio (segreto, NodeId, DataDir);
- apre il firewall su TCP 5000 e UDP 8888.

- **Log**: Visualizzatore eventi → Registri applicazioni → `RaftCore`, oppure un sink configurato in `appsettings.json`.
- **Rimozione**: `.\uninstall-service.ps1`. I dati in `C:\RaftCore\data` restano.
- **Più istanze sulla stessa macchina** (solo test): installare con `-ServiceName RaftCore2`, una directory diversa e `RestPort` diversa. Usare `UdpMode=Multicast` o `Peers`.
