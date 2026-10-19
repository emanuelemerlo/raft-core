using System.Security.Cryptography;
using System.Text;

namespace RaftCore.Services.Http;

/// <summary>REQ-SEC-3: HMAC su metodo, path, timestamp, hash del body, NodeId mittente e nonce.</summary>
public static class RpcAuth
{
    public const string HNode = "X-Cluster-Node", HTime = "X-Cluster-Timestamp", HNonce = "X-Cluster-Nonce", HSig = "X-Cluster-Signature";

    public static string Sign(string secret, string method, string path, long tsMs, ReadOnlySpan<byte> body, string nodeId, string nonce)
    {
        var bodyHash = Convert.ToHexString(SHA256.HashData(body)).ToLowerInvariant();
        var raw = $"{method.ToUpperInvariant()}\n{path}\n{tsMs}\n{bodyHash}\n{nodeId}\n{nonce}";
        return Convert.ToHexString(HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes(raw))).ToLowerInvariant();
    }
}
