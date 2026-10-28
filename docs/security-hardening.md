# Hardening di sicurezza — supply chain e configurazione

Questo documento riassume le modifiche a basso rischio introdotte dal branch `security/harden-ci-config`
(quasi tutte configurazione), il loro **impatto operativo** e i **follow-up** che richiedono uno step manuale
o una scelta esplicita. Non tocca il nucleo Raft.

## Cosa cambia

### 1. GitHub Action pinnate al commit SHA
In `ci.yml`, `release.yml`, `wiki.yml` i riferimenti `@v4`/`@v6` sono sostituiti dal commit SHA, con il tag
di versione come commento. Un tag è un puntatore spostabile: pinnare al SHA impedisce che un tag compromesso
esegua codice arbitrario nella pipeline (che in release ha `contents: write` + `packages: write`).

- **Impatto operativo:** nessuno a runtime. Gli aggiornamenti delle action ora arrivano come PR di Dependabot.

### 2. Dependabot (`.github/dependabot.yml`)
Tiene aggiornati NuGet, GitHub Action e immagine base del container. Necessario perché il pinning al SHA,
da solo, congela anche le patch di sicurezza.

- **Impatto operativo:** PR settimanali da rivedere e mergiare. Minori/patch raggruppate per ridurre il rumore.

### 3. Sorgente NuGet fissata (`NuGet.config`)
Una sola sorgente (nuget.org) con package source mapping su `*`: chiude il rischio di dependency confusion
da feed non previsti, incluse le dipendenze transitive.

- **Impatto operativo:** nessuno, se si usa già nuget.org. Un feed privato andrebbe aggiunto qui esplicitamente.

### 4. Generazione dei lockfile (`Directory.Build.props`)
`RestorePackagesWithLockFile=true`: ogni `dotnet restore` genera/aggiorna i `packages.lock.json`, fissando
anche le versioni transitive.

- **Impatto operativo:** ogni cambio di dipendenza voluto rigenera il lockfile.
- **FOLLOW-UP MANUALE (serve il .NET SDK):**
  1. `dotnet restore RaftCore.sln` per generare i `packages.lock.json`.
  2. Committare i file generati (uno per progetto).
  3. Abilitare il restore bloccato togliendo il commento in `ci.yml` (step *Restore*): `dotnet restore RaftCore.sln --locked-mode`.
     Da quel momento la build **fallisce** su qualsiasi deriva delle dipendenze rispetto al lockfile.

  > Questo step è lasciato manuale di proposito: i lockfile contengono gli hash dei pacchetti risolti e vanno
  > generati da un restore reale, non scritti a mano.

### 5. Endpoint informativi ristretti a loopback
`GET /api/v1/cluster/status` e `/api/v1/metrics` rispondono con **403** se la richiesta non proviene da loopback,
salvo abilitazione esplicita. Nuovo flag `RaftCluster:Management:ExposeStatusEndpoints` (default `false`).
`appsettings.Development.json` lo imposta a `true` per non ostacolare sviluppo e simulatore.
Gli endpoint `/health/*` non sono toccati.

- **Impatto operativo:** un sistema di monitoraggio che faceva scraping di `/metrics` o `/status` **da rete**
  smette di riceverli finché non si imposta `ExposeStatusEndpoints=true` (limitando l'accesso a monte) o non si
  porta lo scraper su loopback/rete di gestione.
- **Dietro reverse proxy:** l'indirizzo visto è quello del proxy, non più loopback → serve `ExposeStatusEndpoints=true`
  e restrizione dell'accesso a livello di proxy.
- Il simulatore interroga i nodi su `127.0.0.1` (loopback): continua a funzionare senza modifiche.

### 6. TLS verso SQL nel template di produzione
`deploy/systemd/raftcore.env.example` ora usa `Encrypt=True;TrustServerCertificate=False`.

- **Impatto operativo:** SQL Server deve presentare un certificato emesso da una CA fidata dai nodi;
  altrimenti la connessione fallisce (è il comportamento voluto: niente connessioni non verificate).
- Gli artefatti di **sviluppo/test** (`docker-compose.yml`, job SQL in `ci.yml`) restano con
  `Encrypt=False`/`TrustServerCertificate=True` perché usano un container SQL senza certificato. Non copiarli in produzione.

### 7. Via i default deboli dal compose
`docker-compose.yml` non ha più un `CLUSTER_SECRET` né una password SA di default: le variabili sono obbligatorie
(`${VAR:?...}`) e l'avvio fallisce con un messaggio chiaro se mancano. Aggiunto `deploy/docker/.env.example`.

- **Impatto operativo:** prima di `docker compose up` bisogna esportare `CLUSTER_SECRET` e `SA_PASSWORD`
  (o definirli in un file `.env`).

## Cosa NON è incluso (follow-up più onerosi, da pianificare)

- **mTLS tra i nodi** e relativa PKI (cifratura del piano dati/controllo; oggi HMAC dà integrità, non riservatezza).
- **Autenticazione di `/api/v1/data`** (oggi chiunque raggiunga la porta può scrivere).
- **Firma degli artefatti di release** (cosign/Sigstore) e attestazione di provenienza (SLSA).
- **Pin per digest** delle immagini base nel `Dockerfile` (Dependabot `docker` è già configurato per gestirlo).
- **CodeQL** come analisi statica in CI.
- **Cifratura a riposo** dei payload (WAL/SQL), da valutare contro il costo di gestione chiavi/recuperabilità.
