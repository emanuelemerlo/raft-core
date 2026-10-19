# Immagine RaftCore (linux/amd64, linux/arm64)
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG VERSION=0.0.0-dev
WORKDIR /src
COPY Directory.Build.props global.json ./
COPY sql/ sql/
COPY src/RaftCore/RaftCore.csproj src/RaftCore/
RUN dotnet restore src/RaftCore/RaftCore.csproj
COPY src/ src/
RUN dotnet publish src/RaftCore/RaftCore.csproj -c Release -o /app --no-restore -p:Version=${VERSION}

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .
# Dati locali del nodo (WAL e stato Raft): montare un volume persistente per nodo
ENV RaftCluster__Storage__DataDir=/data \
    RaftCluster__BaseDirectory=/app \
    RaftCluster__RestPort=5000 \
    DOTNET_gcServer=0
RUN mkdir -p /data && chown -R app:app /data
USER app
VOLUME ["/data"]
EXPOSE 5000 8888/udp
# Health: GET /health/live (processo) e /health/ready (ruolo operativo); configurare la sonda nell'orchestratore.
ENTRYPOINT ["dotnet", "RaftCore.dll"]
