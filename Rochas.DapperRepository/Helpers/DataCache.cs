using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace Rochas.DapperRepository.Helpers
{
    /// <summary>
    /// Cache estático com suporte a provedores plugáveis (InMemory, Distributed, Composite).
    ///
    /// Uso básico (backward compatible):
    ///   DataCache.Initialize(100); // mantém comportamento atual (InMemory)
    ///   DataCache.Put(entity, data);
    ///   var result = DataCache.Get(entity);
    ///
    /// Uso com cache distribuído (Redis/Garnet):
    ///   DataCache.Initialize(new DistributedCacheProvider("localhost:6379"));
    ///   // ou
    ///   DataCache.Initialize(new CompositeCacheProvider(
    ///       new InMemoryCacheProvider(),
    ///       new DistributedCacheProvider("localhost:6379")));
    /// </summary>
    public static class DataCache
    {
        #region Declarations

        private static ICacheProvider _provider;
        private static readonly object _lock = new object();

        public static int MemorySizeLimit;

        #endregion

        #region Public Methods

        /// <summary>Inicializa com InMemoryCacheProvider (backward compatible).</summary>
        public static void Initialize(int memorySizeLimit)
        {
            MemorySizeLimit = memorySizeLimit;
            _provider = new InMemoryCacheProvider(memorySizeLimit);
        }

        /// <summary>Inicializa com provider customizado (Redis/Garnet/Composite).</summary>
        public static void Initialize(ICacheProvider provider)
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        /// <summary>Inicializa com o provider padrão (InMemory).</summary>
        public static void Initialize()
        {
            _provider = new InMemoryCacheProvider();
        }

        public static object Get(object cacheKey)
        {
            EnsureInitialized();
            return _provider.Get(cacheKey);
        }

        public static void Put(object cacheKey, object cacheItem)
        {
            EnsureInitialized();
            _provider.Put(cacheKey, cacheItem);
        }

        public static void Del(object cacheKey, bool deleteAll = false)
        {
            EnsureInitialized();
            _provider.Del(cacheKey, deleteAll);
        }

        public static void Clear()
        {
            EnsureInitialized();
            _provider.Clear();
        }

        /// <summary>Retorna o provider ativo (útil para diagnóstico).</summary>
        public static ICacheProvider GetProvider()
        {
            EnsureInitialized();
            return _provider;
        }

        #endregion

        #region Helper Methods

        private static void EnsureInitialized()
        {
            if (_provider == null)
            {
                lock (_lock)
                {
                    if (_provider == null)
                        _provider = new InMemoryCacheProvider(MemorySizeLimit);
                }
            }
        }

        #endregion
    }
}