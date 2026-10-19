# Contratto client

## Richiesta

```http
POST /api/v1/data
X-Request-ID: <UUID generato UNA volta per operazione logica>
Content-Type: application/json

{ ...oggetto JSON... }
```

- Il client può chiamare **qualsiasi** nodo; non deve sapere chi è il Leader.
- `X-Request-ID` è obbligatorio e identifica l'operazione: **ogni ritento deve riusare lo stesso ID e lo stesso body, byte per byte**. Anche una spaziatura JSON diversa produce 422, perché l'idempotenza confronta l'hash SHA-256 dei byte grezzi.

## Risposte

| Codice | Significato | Il client deve |
| --- | --- | --- |
| 200 | Scritto (o già scritto: header `X-Idempotent-Replay: true`) | considerare l'operazione completata |
| 400 | `X-Request-ID` mancante o non UUID, JSON non valido | correggere la richiesta |
| 410 | La richiesta è rimasta in sospeso oltre `MaxPendingAgeMinutes` e **non** è stata scritta | rigenerare l'operazione con un **nuovo** ID, se è ancora valida |
| 413 | Body oltre `MaxBodyBytes` | ridurre il body |
| 422 | ID riusato con un body diverso, oppure dato rifiutato dal database (`rejected`), oppure messo in quarantena (`quarantined`) | non ritentare; vedere `error`/`reason` nel body |
| 503 | Nessun Leader con quorum entro il deadline, oppure sovraccarico (`Retry-After`) | ritentare con lo stesso ID |
| 504 | Inoltrata, esito non osservato entro il deadline | ritentare con lo stesso ID |

**Importante:** 503, 504 o un timeout **non** significano che il dato non sia stato scritto. Il retry con lo stesso ID è sempre sicuro: restituisce l'esito originale.

## Strategia di retry consigliata

- Ritentare su errore di rete, timeout, 503 e 504; non ritentare sui 4xx.
- Backoff esponenziale con jitter (es. 200 ms, 400 ms, 800 ms… fino a 5 s), passando a un altro nodo se la connessione fallisce.
- Timeout del client almeno `RequestDeadlineMs` + 5 s (default 15 s).
- **Smettere di ritentare prima di `MaxPendingAgeMinutes`** (default 24 h). Oltre quella soglia l'esito di una voce mai completata è 410.

## Esempio C#

```csharp
var id = Guid.NewGuid().ToString();                  // una volta per operazione
var body = JsonSerializer.SerializeToUtf8Bytes(dato); // serializzato UNA volta
foreach (var delay in new[] { 0, 200, 400, 800, 1600, 3200, 5000 })
{
    await Task.Delay(delay);
    var node = nodes[Random.Shared.Next(nodes.Length)];
    try
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, $"{node}/api/v1/data") { Content = new ByteArrayContent(body) };
        req.Headers.Add("X-Request-ID", id);
        req.Content.Headers.ContentType = new("application/json");
        using var res = await http.SendAsync(req);
        if (res.IsSuccessStatusCode) return;                                     // 200
        if ((int)res.StatusCode is >= 400 and < 500) throw new InvalidOperationException(await res.Content.ReadAsStringAsync());
    }
    catch (Exception e) when (e is HttpRequestException or TaskCanceledException) { /* nodo irraggiungibile o timeout: si riprova */ }
}
```
