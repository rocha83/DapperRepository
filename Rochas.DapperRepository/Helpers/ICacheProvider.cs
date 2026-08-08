using System;
using System.Collections.Generic;

namespace Rochas.DapperRepository.Helpers
{
    /// <summary>
    /// Interface abstrata para provedores de cache.
    /// Permite trocar entre InMemory (ConcurrentDictionary) e distribuído (Redis/Garnet)
    /// sem alterar o código do repositório.
    ///
    /// Implementações:
    ///   - InMemoryCacheProvider: comportamento atual (ConcurrentDictionary)
    ///   - DistributedCacheProvider: Redis/Garnet via IDistributedCache do Microsoft.Extensions
    ///   - CompositeCacheProvider: L1 (in-memory) + L2 (distribuído) para alta disponibilidade
    /// </summary>
    public interface ICacheProvider
    {
        /// <summary>Recupera item do cache pela chave serializada.</summary>
        object Get(object cacheKey);

        /// <summary>Armazena item no cache com chave serializada.</summary>
        void Put(object cacheKey, object cacheItem);

        /// <summary>Remove item específico do cache. Se deleteAll=true, remove todos do mesmo tipo.</summary>
        void Del(object cacheKey, bool deleteAll = false);

        /// <summary>Limpa todo o cache.</summary>
        void Clear();
    }
}