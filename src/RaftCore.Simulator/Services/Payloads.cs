using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace RaftCore.Simulator.Services;

/// <summary>
/// Costruisce il JSON da inviare a ogni endpoint. Ogni endpoint ha un modello (oggetto JSON) i cui valori stringa possono
/// contenere segnaposto; i campi di servizio seq, timestamp, source e runId sono sempre presenti, perché la verifica delle
/// scritture li usa. Segnaposto:
/// <list type="bullet">
/// <item><c>{seq}</c> numero progressivo dell'operazione; <c>{now}</c> istante di invio (ISO 8601); <c>{uuid}</c> GUID nuovo;</item>
/// <item><c>{random}</c> decimale 0-100 con 2 cifre; <c>{randomInt}</c> intero 0-1000; <c>{choice:a|b|c}</c> uno dei valori.</item>
/// </list>
/// Una stringa che è esattamente <c>{seq}</c>, <c>{random}</c> o <c>{randomInt}</c> diventa un numero JSON.
/// I valori casuali dipendono da seq: la stessa operazione produce sempre lo stesso body.
/// </summary>
public sealed partial class PayloadFactory
{
    private static readonly HashSet<string> ServiceFields = new(StringComparer.Ordinal) { "seq", "timestamp", "source", "runId", "scenario", "pad" };
    private readonly ConcurrentDictionary<string, JsonObject> _templates = new(StringComparer.Ordinal);
    private readonly SimulatorOptions _opt;

    public PayloadFactory(SimulatorOptions opt)
    {
        _opt = opt;
        foreach (var (name, fields) in opt.Payloads)
        {
            var o = new JsonObject();
            foreach (var (k, v) in fields) o[k] = LiteralOrString(v);
            _templates[name] = o;
        }
    }

    /// <summary>Modelli correnti, come testo JSON indentato (per l'interfaccia). Un endpoint senza modello usa {}.</summary>
    public IReadOnlyDictionary<string, string> Templates(IEnumerable<string> endpoints) =>
        endpoints.Distinct().ToDictionary(e => e, e => _templates.TryGetValue(e, out var t)
            ? t.ToJsonString(new JsonSerializerOptions { WriteIndented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping })
            : "{}");

    /// <summary>Sostituisce il modello di un endpoint; deve essere un oggetto JSON.</summary>
    public void SetTemplate(string endpoint, string json)
    {
        JsonNode? node;
        try { node = JsonNode.Parse(json); }
        catch (JsonException ex) { throw new SimulatorException($"Modello di '{endpoint}' non valido: {ex.Message}"); }
        if (node is not JsonObject o) throw new SimulatorException($"Il modello di '{endpoint}' deve essere un oggetto JSON");
        var reserved = o.Select(kv => kv.Key).Where(ServiceFields.Contains).ToList();
        if (reserved.Count > 0) throw new SimulatorException($"Campi riservati al simulatore nel modello di '{endpoint}': {string.Join(", ", reserved)}");
        _templates[endpoint] = o;
    }

    /// <summary>Body dell'operazione: campi di servizio, poi il modello dell'endpoint, poi l'eventuale riempimento.</summary>
    public byte[] Build(string endpoint, long seq, DateTimeOffset at, string runId, int padBytes = 0, string? scenario = null)
    {
        var rnd = new Random(unchecked((int)(seq * 2654435761L ^ endpoint.GetHashCode(StringComparison.Ordinal))));
        using var ms = new MemoryStream();
        using (var w = new Utf8JsonWriter(ms))
        {
            w.WriteStartObject();
            w.WriteNumber("seq", seq);
            w.WriteString("timestamp", at);
            w.WriteString("source", _opt.Source);
            w.WriteString("runId", runId);
            if (scenario != null) w.WriteString("scenario", scenario);
            if (_templates.TryGetValue(endpoint, out var t))
                foreach (var (k, v) in t)
                {
                    if (ServiceFields.Contains(k)) continue;
                    w.WritePropertyName(k);
                    if (Resolve(v, seq, at, rnd) is { } value) value.WriteTo(w); else w.WriteNullValue();
                }
            if (padBytes > 0) w.WriteString("pad", new string('x', padBytes));
            w.WriteEndObject();
        }
        return ms.ToArray();
    }

    private static JsonNode? Resolve(JsonNode? node, long seq, DateTimeOffset at, Random rnd) => node switch
    {
        JsonObject o => new JsonObject(o.Select(kv => KeyValuePair.Create(kv.Key, Resolve(kv.Value, seq, at, rnd)))),
        JsonArray a => new JsonArray(a.Select(x => Resolve(x, seq, at, rnd)).ToArray()),
        JsonValue v when v.TryGetValue<string>(out var s) => ResolveString(s, seq, at, rnd),
        null => null,
        _ => node.DeepClone()
    };

    private static JsonNode ResolveString(string s, long seq, DateTimeOffset at, Random rnd)
    {
        switch (s)
        {
            case "{seq}": return JsonValue.Create(seq);
            case "{random}": return JsonValue.Create(Math.Round(rnd.NextDouble() * 100, 2));
            case "{randomInt}": return JsonValue.Create(rnd.Next(0, 1001));
        }
        return JsonValue.Create(Placeholder().Replace(s, m => m.Groups[1].Value switch
        {
            "seq" => seq.ToString(CultureInfo.InvariantCulture),
            "now" => at.ToString("O", CultureInfo.InvariantCulture),
            "uuid" => Guid.NewGuid().ToString("D"),
            "random" => Math.Round(rnd.NextDouble() * 100, 2).ToString(CultureInfo.InvariantCulture),
            "randomInt" => rnd.Next(0, 1001).ToString(CultureInfo.InvariantCulture),
            var c when c.StartsWith("choice:", StringComparison.Ordinal) && c.Length > 7 => Pick(c[7..].Split('|'), rnd),
            _ => m.Value
        }))!;
    }

    private static string Pick(string[] options, Random rnd) => options[rnd.Next(options.Length)];

    /// <summary>Valore da configurazione: numeri e booleani restano tipizzati, il resto è una stringa (anche con segnaposto).</summary>
    private static JsonNode? LiteralOrString(string v)
    {
        if (bool.TryParse(v, out var b)) return JsonValue.Create(b);
        if (decimal.TryParse(v, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) && !v.StartsWith('{')) return JsonValue.Create(d);
        return JsonValue.Create(v);
    }

    [GeneratedRegex(@"\{(seq|now|uuid|random|randomInt|choice:[^}]*)\}")]
    private static partial Regex Placeholder();
}
