-- RaftCore - tabella di un endpoint per il writer JSON di default (Endpoints[].Table), schema versione 2 (v0.3)
-- Idempotente. Esecuzione manuale, una volta per endpoint:
--   sqlcmd -S <server> -d <database> -i 002_endpoint_table.sql -v Schema=dbo Table=Misure
-- Con AutoCreateSchema=true il servizio esegue lo stesso script per ogni endpoint senza writer personalizzato.

IF SCHEMA_ID(N'$(Schema)') IS NULL EXEC(N'CREATE SCHEMA [$(Schema)]');
GO

IF OBJECT_ID(N'[$(Schema)].[$(Table)]', N'U') IS NULL
CREATE TABLE [$(Schema)].[$(Table)] (
    Id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_$(Table)] PRIMARY KEY,
    ClusterId     NVARCHAR(100)    NOT NULL,
    RequestId     UNIQUEIDENTIFIER NOT NULL,
    LogIndex      BIGINT           NOT NULL,
    ReceivedUtc   DATETIME2(3)     NOT NULL,
    Payload       NVARCHAR(MAX)    NOT NULL CONSTRAINT [CK_$(Table)_Json] CHECK (ISJSON(Payload) = 1),
    CONSTRAINT [UQ_$(Table)_Request] UNIQUE (ClusterId, RequestId)
);
GO
