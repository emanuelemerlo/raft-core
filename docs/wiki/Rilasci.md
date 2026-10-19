# Creare una release

1. Aggiornare `CHANGELOG.md`: spostare le voci di `[Unreleased]` in una nuova sezione `## [X.Y.Z] - AAAA-MM-GG`. Il testo di quella sezione diventa la nota di rilascio.
2. Aggiornare `<Version>` in `Directory.Build.props`. Il workflow usa comunque la versione del tag.
3. Unire su `main` con CI verde, poi creare e pubblicare il tag:

```bash
git tag v0.2.0
git push origin v0.2.0
```

Il workflow **Release**:
- esegue i test;
- produce i pacchetti self-contained a file singolo (`win-x64`, `linux-x64`, `linux-arm64`, `osx-arm64`, `osx-x64`), ognuno con README, CHANGELOG e script di deploy;
- calcola `SHA256SUMS.txt`;
- pubblica l'immagine `ghcr.io/emanuelemerlo/raft-core:X.Y.Z` (e `latest`);
- crea la release GitHub con le note estratte.

Un tag con suffisso (es. `v0.3.0-rc.1`) crea una *pre-release*. Per provare la pipeline senza pubblicare: Actions → Release → *Run workflow* con una versione di prova.

## Wiki

Le pagine di `docs/wiki` vengono pubblicate nella Wiki a ogni modifica su `main` (workflow **Wiki**). Prerequisito una tantum: abilitare la Wiki nelle impostazioni del repository e creare una prima pagina qualsiasi dall'interfaccia.
