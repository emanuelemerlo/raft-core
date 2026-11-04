-- RaftCore - tabelle di servizio del database condiviso (state machine), schema versione 2 (v0.3)
-- Idempotente: può essere rieseguito. Lo schema [raft] è sostituito dal valore di StateMachine:SqlServer:Schema
-- quando lo script viene eseguito automaticamente (AutoCreateSchema=true).
-- Esecuzione manuale: sqlcmd -S <server> -d <database> -i 001_init.sql
-- Le tabelle degli endpoint si creano con 002_endpoint_table.sql.
-- Nessuna migrazione da versioni precedenti: con tabelle di servizio di un'altra versione lo script si interrompe.

IF SCHEMA_ID(N'raft') IS NULL EXEC(N'CREATE SCHEMA [raft]');
GO

-- Tabelle della v0.2 (senza SchemaVersion) o di un'altra versione: errore esplicito, nessuna modifica.
IF OBJECT_ID(N'[raft].[SchemaVersion]', N'U') IS NULL
BEGIN
    IF OBJECT_ID(N'[raft].[ApplyState]', N'U') IS NOT NULL
        THROW 50100, 'SchemaVersion: lo schema [raft] contiene tabelle di servizio di una versione precedente di RaftCore (attesa 2). Usare un database o uno schema nuovo.', 1;
END
ELSE IF EXISTS (SELECT 1 FROM [raft].[SchemaVersion] WHERE Version <> 2)
BEGIN
    THROW 50100, 'SchemaVersion: lo schema [raft] contiene tabelle di servizio di un''altra versione di RaftCore (attesa 2). Usare un database o uno schema nuovo.', 1;
END
GO

-- Versione dello schema delle tabelle di servizio: una sola riga.
IF OBJECT_ID(N'[raft].[SchemaVersion]', N'U') IS NULL
CREATE TABLE [raft].[SchemaVersion] (
    Version       INT          NOT NULL CONSTRAINT PK_SchemaVersion PRIMARY KEY,
    CreatedUtc    DATETIME2(3) NOT NULL
);
GO
IF NOT EXISTS (SELECT 1 FROM [raft].[SchemaVersion]) INSERT INTO [raft].[SchemaVersion] (Version, CreatedUtc) VALUES (2, SYSUTCDATETIME());
GO

-- Una riga per cluster: indice dell'ultima voce del log Raft applicata al database.
-- Ogni apply aggiorna questa riga NELLA STESSA transazione della scrittura dei dati,
-- con la condizione LastApplied = LogIndex - 1: nessuna voce può essere scritta due volte o fuori ordine.
IF OBJECT_ID(N'[raft].[ApplyState]', N'U') IS NULL
CREATE TABLE [raft].[ApplyState] (
    ClusterId     NVARCHAR(100) NOT NULL CONSTRAINT PK_ApplyState PRIMARY KEY,
    LastApplied   BIGINT        NOT NULL,
    UpdatedUtc    DATETIME2(3)  NOT NULL,
    UpdatedBy     NVARCHAR(64)  NULL
);
GO

-- Esito di ogni voce applicata (anche NoOp, duplicati, conflitti, scadute, rifiutate, in quarantena):
-- è la fonte dei replay idempotenti.
IF OBJECT_ID(N'[raft].[AppliedEntries]', N'U') IS NULL
CREATE TABLE [raft].[AppliedEntries] (
    ClusterId     NVARCHAR(100)    NOT NULL,
    LogIndex      BIGINT           NOT NULL,
    RequestId     UNIQUEIDENTIFIER NULL,
    PayloadType   NVARCHAR(64)     NULL,      -- nome dell'endpoint; NULL per le voci NoOp
    Status        TINYINT          NOT NULL,  -- 1 Applied, 2 Duplicate, 3 Conflict, 4 Rejected, 5 Expired, 6 Quarantined, 7 NoOp
    HttpStatus    SMALLINT         NOT NULL,
    ResponseBody  VARBINARY(MAX)   NULL,
    AppliedUtc    DATETIME2(3)     NOT NULL,
    CONSTRAINT PK_AppliedEntries PRIMARY KEY (ClusterId, LogIndex)
);
GO

-- Prima occorrenza di ogni RequestId: indice di idempotenza permanente (REQ-IDM), anche dopo la compattazione del log.
-- Unico per cluster su tutti gli endpoint (REQ-EPT-3); PayloadHash copre anche il nome dell'endpoint.
IF OBJECT_ID(N'[raft].[Requests]', N'U') IS NULL
CREATE TABLE [raft].[Requests] (
    ClusterId     NVARCHAR(100)    NOT NULL,
    RequestId     UNIQUEIDENTIFIER NOT NULL,
    PayloadType   NVARCHAR(64)     NOT NULL,
    PayloadHash   CHAR(64)         NOT NULL,
    LogIndex      BIGINT           NOT NULL,
    Status        TINYINT          NOT NULL,
    ReceivedUtc   DATETIME2(3)     NOT NULL,
    CONSTRAINT PK_Requests PRIMARY KEY (ClusterId, RequestId)
);
GO

-- Voci messe in quarantena (poison entry): payload conservato per analisi e riprocessamento manuale.
IF OBJECT_ID(N'[raft].[Quarantine]', N'U') IS NULL
CREATE TABLE [raft].[Quarantine] (
    ClusterId     NVARCHAR(100)    NOT NULL,
    LogIndex      BIGINT           NOT NULL,
    RequestId     UNIQUEIDENTIFIER NULL,
    PayloadType   NVARCHAR(64)     NULL,
    Payload       VARBINARY(MAX)   NULL,
    Reason        NVARCHAR(2000)   NOT NULL,
    CreatedUtc    DATETIME2(3)     NOT NULL,
    CONSTRAINT PK_Quarantine PRIMARY KEY (ClusterId, LogIndex)
);
GO
