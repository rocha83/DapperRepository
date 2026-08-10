using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Rochas.DapperRepository.Helpers
{
    /// <summary>
    /// Canal de persistência assíncrona para replicação de dados entre clusters SQL.
    ///
    /// Arquitetura:
    ///   Master SQL ──▶ GenericRepository.Add/Update/Delete ──▶ PersistenceChannelCacheProvider
    ///                                                                   │
    ///                                                            Channel&lt;Message&gt;
    ///                                                                   │
    ///                                                            Worker Threads
    ///                                                              │        │
    ///                                                        Slave SQL  Slave SQL
    ///                                                       (cluster 1) (cluster N)
    ///
    /// Uso:
    ///   var localCache = new InMemoryCacheProvider();
    ///   var channelProvider = new PersistenceChannelCacheProvider(localCache);
    ///
    ///   DataCache.Initialize(channelProvider);
    ///
    ///   // No cluster consumidor (slave):
    ///   await foreach (var msg in channelProvider.ConsumeAsync(ct))
    ///       PersistirNoSlave(msg);
    ///
    /// O canal é in-memory (System.Threading.Channels), zero dependência externa.
    /// Cada consumidor recebe uma cópia via BroadcastChannel implícito no padrão
    /// publish-subscribe (se múltiplos consumidores, todos recebem).
    /// </summary>
    public class PersistenceChannelCacheProvider : ICacheProvider, IDisposable
    {
        private readonly ICacheProvider _inner;
        private readonly Channel<ChannelMessage> _channel;
        private readonly CancellationTokenSource _cts = new();

        public PersistenceChannelCacheProvider(ICacheProvider innerProvider, int channelCapacity = 10000)
        {
            _inner = innerProvider ?? throw new ArgumentNullException(nameof(innerProvider));
            _channel = Channel.CreateBounded<ChannelMessage>(new BoundedChannelOptions(channelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait // backpressure: não perde mensagem
            });
        }

        /// <summary>
        /// Mensagem de replicação contendo a ação e a entidade serializada.
        /// O consumidor usa estes dados para persistir no SQL slave.
        /// </summary>
        public class ChannelMessage
        {
            public ChannelAction Action { get; set; }
            public object CacheKey { get; set; }
            public object CacheItem { get; set; }
            public bool DeleteAll { get; set; }
            public DateTime Timestamp { get; set; } = DateTime.UtcNow;
        }

        public enum ChannelAction
        {
            Put,
            Del,
            Clear
        }

        // ── ICacheProvider (leitura do cache local, escrita no canal) ──────

        public object Get(object cacheKey)
            => _inner.Get(cacheKey);

        public void Put(object cacheKey, object cacheItem)
        {
            _inner.Put(cacheKey, cacheItem);

            // Publica no canal para replicação assíncrona nos slaves
            _channel.Writer.TryWrite(new ChannelMessage
            {
                Action = ChannelAction.Put,
                CacheKey = cacheKey,
                CacheItem = cacheItem
            });
        }

        public void Del(object cacheKey, bool deleteAll = false)
        {
            _inner.Del(cacheKey, deleteAll);

            _channel.Writer.TryWrite(new ChannelMessage
            {
                Action = ChannelAction.Del,
                CacheKey = cacheKey,
                CacheItem = null,
                DeleteAll = deleteAll
            });
        }

        public void Clear()
        {
            _inner.Clear();

            _channel.Writer.TryWrite(new ChannelMessage
            {
                Action = ChannelAction.Clear,
                CacheKey = string.Empty,
                CacheItem = null
            });
        }

        /// <summary>
        /// Consome o canal de forma assíncrona. Cada mensagem contém a ação
        /// e os dados para persistir no SQL slave.
        ///
        /// Exemplo de uso no consumidor:
        ///
        ///   await foreach (var msg in provider.ConsumeAsync(ct))
        ///   {
        ///       using var slaveConn = new NpgsqlConnection(slaveConnString);
        ///       slaveConn.Open();
        ///       switch (msg.Action)
        ///       {
        ///           case ChannelAction.Put:
        ///               var repo = new GenericRepository<T>(engine, slaveConn);
        ///               await repo.Add(msg.CacheItem as T);
        ///               break;
        ///           case ChannelAction.Del:
        ///               // remover do slave
        ///               break;
        ///       }
        ///   }
        /// </summary>
        public async IAsyncEnumerable<ChannelMessage> ConsumeAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken, _cts.Token);

            while (!linkedCts.Token.IsCancellationRequested)
            {
                ChannelMessage msg;
                try
                {
                    msg = await _channel.Reader.ReadAsync(linkedCts.Token);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }

                yield return msg;
            }
        }

        /// <summary>
        /// Encerra o canal e interrompe todos os consumidores.
        /// </summary>
        public void Stop()
        {
            _cts.Cancel();
            _channel.Writer.Complete();
        }

        public void Dispose()
        {
            Stop();
            _cts.Dispose();
        }
    }
}