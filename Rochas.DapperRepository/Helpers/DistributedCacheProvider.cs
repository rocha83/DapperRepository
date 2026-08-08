using System;
using System.Text.Json;

namespace Rochas.DapperRepository.Helpers
{
    /// <summary>
    /// Provedor de cache distribuído para Redis ou Microsoft Garnet.
    ///
    /// Para usar:
    ///   1. Instale o pacote NuGet apropriado:
    ///      - Microsoft.Extensions.Caching.StackExchangeRedis (Redis)
    ///      - ou Microsoft.Garnet (Garnet in-memory Redis-compatible)
    ///   2. Implemente o construtor com sua connection string
    ///   3. Configure no DataCache.Initialize() ou no construtor do repositório
    ///
    /// Garnet é in-memory Redis-compatible da Microsoft, ideal para cenários
    /// onde se quer performance in-memory com协议 Redis (compatível com clientes Redis).
    ///
    /// Exemplo de uso:
    ///   DataCache.Initialize(new DistributedCacheProvider("localhost:6379"));
    ///   // ou
    ///   DataCache.Initialize(new DistributedCacheProvider("localhost:6380", "senha"));
    /// </summary>
    public class DistributedCacheProvider : ICacheProvider
    {
        private readonly string _connectionString;
        private readonly string _instanceName;
        private readonly TimeSpan? _defaultExpiration;

        // TODO: Implementar com Microsoft.Extensions.Caching.StackExchangeRedis.IDistributedCache
        // ou com Microsoft.Garnet (Redis-compatible)
        //
        // private IDistributedCache _distributedCache;
        //
        // public DistributedCacheProvider(string connectionString, string instanceName = "dapper:",
        //     TimeSpan? defaultExpiration = null)
        // {
        //     _connectionString = connectionString;
        //     _instanceName = instanceName;
        //     _defaultExpiration = defaultExpiration ?? TimeSpan.FromMinutes(5);
        //
        //     var options = ConfigurationOptions.Parse(connectionString);
        //     var multiplexer = ConnectionMultiplexer.Connect(options);
        //     _distributedCache = new RedisCache(new RedisCacheOptions
        //     {
        //         Configuration = connectionString,
        //         InstanceName = instanceName
        //     });
        // }

        public DistributedCacheProvider(string connectionString, string instanceName = "dapper:",
            TimeSpan? defaultExpiration = null)
        {
            _connectionString = connectionString;
            _instanceName = instanceName;
            _defaultExpiration = defaultExpiration ?? TimeSpan.FromMinutes(5);
        }

        public object Get(object cacheKey)
        {
            if (cacheKey == null) return null;

            // TODO: Implementar com _distributedCache.GetAsync
            // var key = BuildKey(cacheKey);
            // var bytes = _distributedCache.Get(key);
            // if (bytes == null) return null;
            // return JsonSerializer.Deserialize<object>(bytes);

            throw new NotImplementedException(
                "DistributedCacheProvider requer implementação. " +
                "Instale Microsoft.Extensions.Caching.StackExchangeRedis ou Microsoft.Garnet.");
        }

        public void Put(object cacheKey, object cacheItem)
        {
            if (cacheKey == null || cacheItem == null) return;

            // TODO: Implementar com _distributedCache.SetAsync
            // var key = BuildKey(cacheKey);
            // var bytes = JsonSerializer.SerializeToUtf8Bytes(cacheItem);
            // _distributedCache.Set(key, bytes, new DistributedCacheEntryOptions
            // {
            //     AbsoluteExpirationRelativeToNow = _defaultExpiration
            // });

            throw new NotImplementedException(
                "DistributedCacheProvider requer implementação. " +
                "Instale Microsoft.Extensions.Caching.StackExchangeRedis ou Microsoft.Garnet.");
        }

        public void Del(object cacheKey, bool deleteAll = false)
        {
            if (cacheKey == null) return;

            // TODO: Implementar com _distributedCache.Remove
            // Se deleteAll, usar pattern matching (Redis KEYS ou SCAN)
            // var key = BuildKey(cacheKey);
            // _distributedCache.Remove(key);

            throw new NotImplementedException(
                "DistributedCacheProvider requer implementação. " +
                "Instale Microsoft.Extensions.Caching.StackExchangeRedis ou Microsoft.Garnet.");
        }

        public void Clear()
        {
            // TODO: Implementar flush do banco Redis/Garnet
            // ou usar _distributedCache.Clear() se disponível

            throw new NotImplementedException(
                "DistributedCacheProvider requer implementação. " +
                "Instale Microsoft.Extensions.Caching.StackExchangeRedis ou Microsoft.Garnet.");
        }

        private string BuildKey(object cacheKey)
        {
            return _instanceName + JsonSerializer.Serialize(cacheKey);
        }
    }
}