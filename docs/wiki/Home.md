# RaftCore — Wiki

RaftCore riceve dati JSON su uno o più endpoint REST e li scrive una sola volta su SQL Server, restando disponibile quando un'istanza si guasta.

**Iniziare**
- [Avvio rapido](Avvio-rapido) — 3 istanze sulla stessa macchina, prima scrittura, prova di failover
- [Contratto client](Contratto-client) — come chiamare il servizio e come ritentare

**Installare**
- [Windows Service](Deploy-Windows-Service) · [Linux systemd](Deploy-Linux-systemd) · [Docker](Docker)
- [SQL Server](SQL-Server) — schema, writer per endpoint, quarantena

**Gestire**
- [Configurazione](Configurazione) — tutti i parametri
- [Operazioni](Operazioni) — monitoraggio, aggiornamenti, endpoint, sostituzione di un nodo, capacità
- [Risoluzione dei problemi](Risoluzione-problemi)
- [Creare una release](Rilasci)

**Capire**
- [Architettura](Architettura)
- [Specifica](Specifica) — requisiti normativi, garanzie e matrice dei test (rev. 6)
