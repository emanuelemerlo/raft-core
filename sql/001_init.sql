-- RaftCore - schema del database condiviso (state machine), v0.2
-- Idempotente: può essere rieseguito. Lo schema [raft] è sostituito dal valore di StateMachine:SqlServer:Schema
-- quando lo script viene eseguito automaticamente (AutoCreateSchema=true).
-- Esecuzione manuale: sqlcmd -S <server> -d <database> -i 001_init.sql

IF SCHEMA_ID(N'raft') IS NULL EXEC(N'CREATE SCHEMA [raft]');
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
    Status        TINYINT          NOT NULL,  -- 1 Applied, 2 Duplicate, 3 Conflict, 4 Rejected, 5 Expired, 6 Quarantined, 7 NoOp
    HttpStatus    SMALLINT         NOT NULL,
    ResponseBody  VARBINARY(MAX)   NULL,
    AppliedUtc    DATETIME2(3)     NOT NULL,
    CONSTRAINT PK_AppliedEntries PRIMARY KEY (ClusterId, LogIndex)
);
GO

-- Prima occorrenza di ogni RequestId: indice di idempotenza permanente (REQ-IDM), anche dopo la compattazione del log.
IF OBJECT_ID(N'[raft].[Requests]', N'U') IS NULL
CREATE TABLE [raft].[Requests] (
    ClusterId     NVARCHAR(100)    NOT NULL,
    RequestId     UNIQUEIDENTIFIER NOT NULL,
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
    Payload       VARBINARY(MAX)   NULL,
    Reason        NVARCHAR(2000)   NOT NULL,
    CreatedUtc    DATETIME2(3)     NOT NULL,
    CONSTRAINT PK_Quarantine PRIMARY KEY (ClusterId, LogIndex)
);
GO

-- Tabella applicativa del writer di default (JsonPayloadWriter). Sostituibile con le proprie tabelle
-- implementando ISqlPayloadWriter.
IF OBJECT_ID(N'[raft].[IngestedData]', N'U') IS NULL
CREATE TABLE [raft].[IngestedData] (
    Id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_IngestedData PRIMARY KEY,
    ClusterId     NVARCHAR(100)    NOT NULL,
    RequestId     UNIQUEIDENTIFIER NOT NULL,
    LogIndex      BIGINT           NOT NULL,
    ReceivedUtc   DATETIME2(3)     NOT NULL,
    Payload       NVARCHAR(MAX)    NOT NULL CONSTRAINT CK_IngestedData_Json CHECK (ISJSON(Payload) = 1),
    CONSTRAINT UQ_IngestedData_Request UNIQUE (ClusterId, RequestId)
);
GO
