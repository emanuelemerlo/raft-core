using RaftCore.Hosting;

// Avvio come processo console, Windows Service o unità systemd (rilevati automaticamente).
// In ambiente Development con RaftCluster:DevCluster:Instances > 1 questo processo avvia anche gli altri nodi del cluster.
try
{
    await using var node = await NodeHost.StartAsync(args);
    ParentWatchdog.Start(node);                                       // nodo figlio del cluster di sviluppo
    using var devCluster = DevClusterLauncher.StartIfConfigured(node, args);
    await node.App.WaitForShutdownAsync();
    return (int)ExitCodes.Ok;
}
catch (NodeStartException ex)
{
    Console.Error.WriteLine($"ERRORE DI AVVIO ({ex.Code}) - il nodo non parte:");
    Console.Error.WriteLine(ex.Message);
    if (OperatingSystem.IsWindows() && args.Any(a => a.Contains("DevCluster:ParentPid=", StringComparison.OrdinalIgnoreCase)))
    {
        // nodo figlio in una finestra propria: la si tiene aperta per poter leggere l'errore
        Console.Error.WriteLine("La finestra si chiuderà tra 30 secondi.");
        await Task.Delay(30_000);
    }
    return (int)ex.Code;
}
