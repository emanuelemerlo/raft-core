using RaftCore.Hosting;

// Avvio come processo console, Windows Service o unità systemd (rilevati automaticamente).
try
{
    await using var node = await NodeHost.StartAsync(args);
    await node.App.WaitForShutdownAsync();
    return (int)ExitCodes.Ok;
}
catch (NodeStartException ex)
{
    Console.Error.WriteLine($"ERRORE DI AVVIO ({ex.Code}) - il nodo non parte:");
    Console.Error.WriteLine(ex.Message);
    return (int)ex.Code;
}
