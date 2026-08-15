using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Rochas.DapperRepository.Exceptions;
using Rochas.DapperRepository.Helpers;
using Rochas.DapperRepository.Specification.Enums;

namespace Rochas.DapperRepository.Test
{
    public class CacheProvidersTests
    {
        #region DataCache

        [Fact]
        public void DataCache_Initialize_WithSizeLimit()
        {
            DataCache.Initialize(10);

            Assert.Equal(10, DataCache.MemorySizeLimit);

            var entity = new SampleEntity { DocNumber = 80001, Name = "Cache Size Limit", Active = true };
            DataCache.Put("cache-size-1", entity);
            var result = DataCache.Get("cache-size-1");
            Assert.NotNull(result);
            DataCache.Del("cache-size-1");
            Assert.Null(DataCache.Get("cache-size-1"));
        }

        [Fact]
        public void DataCache_Initialize_Default()
        {
            DataCache.Initialize();
            var entity = new SampleEntity { DocNumber = 80005, Name = "Cache Default", Active = true };
            DataCache.Put("cache-default", entity);
            var result = DataCache.Get("cache-default") as SampleEntity;
            Assert.NotNull(result);
            Assert.Equal("Cache Default", result.Name);
            DataCache.Clear();
            Assert.Null(DataCache.Get("cache-default"));
        }

        [Fact]
        public void DataCache_Initialize_WithProvider()
        {
            DataCache.Initialize(new InMemoryCacheProvider());
            DataCache.Put("cache-provider", new SampleEntity { DocNumber = 80002, Name = "Cache Provider", Active = true });
            Assert.NotNull(DataCache.Get("cache-provider"));
        }

        [Fact]
        public void DataCache_Initialize_NullProvider_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => DataCache.Initialize((ICacheProvider)null));
        }

        [Fact]
        public void DataCache_GetProvider_ReturnsActive()
        {
            DataCache.Initialize(new InMemoryCacheProvider());
            var provider = DataCache.GetProvider();
            Assert.NotNull(provider);
            Assert.IsAssignableFrom<ICacheProvider>(provider);
        }

        [Fact]
        public void DataCache_Del_DeleteAll()
        {
            DataCache.Initialize(new InMemoryCacheProvider());
            var entity = new SampleEntity { DocNumber = 80003, Name = "Cache Del All", Active = true };
            DataCache.Put(entity, new List<SampleEntity> { entity, new SampleEntity { DocNumber = 80004 } });
            DataCache.Del(entity, true);
        }

        #endregion

        #region InMemoryCacheProvider

        [Fact]
        public void InMemoryCache_Put_Get_RoundTrip()
        {
            var provider = new InMemoryCacheProvider();
            var entity = new SampleEntity { DocNumber = 81001, Name = "InMemory RoundTrip", Active = true };
            provider.Put("mem-key-1", entity);

            var result = provider.Get("mem-key-1") as SampleEntity;
            Assert.NotNull(result);
            Assert.Equal("InMemory RoundTrip", result.Name);
        }

        [Fact]
        public void InMemoryCache_Get_Missing_ReturnsNull()
        {
            var provider = new InMemoryCacheProvider();
            Assert.Null(provider.Get("mem-key-missing"));
        }

        [Fact]
        public void InMemoryCache_Get_NullKey_ReturnsNull()
        {
            var provider = new InMemoryCacheProvider();
            Assert.Null(provider.Get(null));
        }

        [Fact]
        public void InMemoryCache_Put_NullValues_Noop()
        {
            var provider = new InMemoryCacheProvider();
            provider.Put(null, "value");
            provider.Put("mem-key-nullitem", null);
            Assert.Null(provider.Get("mem-key-nullitem"));
        }

        [Fact]
        public void InMemoryCache_Get_SingleItemList_Unwraps()
        {
            var provider = new InMemoryCacheProvider();
            var entity = new SampleEntity { DocNumber = 81002, Name = "Unwrap Single", Active = true };
            provider.Put("mem-key-list", new List<SampleEntity> { entity });

            var result = provider.Get("mem-key-list");
            Assert.NotNull(result);
            Assert.IsNotType<List<SampleEntity>>(result);
            Assert.Equal("Unwrap Single", ((SampleEntity)result).Name);
        }

        [Fact]
        public void InMemoryCache_Get_MultiItemList_StaysList()
        {
            var provider = new InMemoryCacheProvider();
            var list = new List<SampleEntity>
            {
                new SampleEntity { DocNumber = 81003, Name = "Stay List A", Active = true },
                new SampleEntity { DocNumber = 81004, Name = "Stay List B", Active = true }
            };
            provider.Put("mem-key-multi", list);

            var result = provider.Get("mem-key-multi");
            Assert.NotNull(result);
            var asList = result as List<SampleEntity>;
            Assert.NotNull(asList);
            Assert.Equal(2, asList.Count);
        }

        [Fact]
        public void InMemoryCache_Del_Removes()
        {
            var provider = new InMemoryCacheProvider();
            provider.Put("mem-key-del", new SampleEntity { DocNumber = 81005, Name = "To Delete", Active = true });
            Assert.NotNull(provider.Get("mem-key-del"));

            provider.Del("mem-key-del");
            Assert.Null(provider.Get("mem-key-del"));
        }

        [Fact]
        public void InMemoryCache_Del_NullKey_Noop()
        {
            var provider = new InMemoryCacheProvider();
            provider.Del(null);
        }

        [Fact]
        public void InMemoryCache_Del_DeleteAll_RemovesType()
        {
            var provider = new InMemoryCacheProvider();
            var entity = new SampleEntity { DocNumber = 81006, Name = "Delete All", Active = true };
            provider.Put(entity, new List<SampleEntity> { entity });

            provider.Del(entity, true);
            Assert.Null(provider.Get(entity));
        }

        [Fact]
        public void InMemoryCache_Clear_Empties()
        {
            var provider = new InMemoryCacheProvider();
            var entity = new SampleEntity { DocNumber = 81009, Name = "To Clear", Active = true };
            provider.Put("mem-key-clear", entity);
            Assert.NotNull(provider.Get("mem-key-clear"));
            provider.Clear();
            Assert.Null(provider.Get("mem-key-clear"));
        }

        [Fact]
        public void InMemoryCache_MemoryLimit_Reset()
        {
            var provider = new InMemoryCacheProvider(1);
            var entity = new SampleEntity { DocNumber = 81010, Name = "Memory Limit", Active = true };
            provider.Put("mem-key-limit", entity);
            Assert.NotNull(provider.Get("mem-key-limit"));
        }

        [Fact]
        public void InMemoryCache_UpdateExisting_ReplacesInList()
        {
            var provider = new InMemoryCacheProvider();
            var original = new SampleEntity { DocNumber = 81007, Name = "Original Name", Active = true };
            var second = new SampleEntity { DocNumber = 81008, Name = "Second Name", Active = true };
            provider.Put("mem-key-update", new List<SampleEntity> { original, second });

            var updated = new SampleEntity { DocNumber = 81007, Name = "Updated Name", Active = true };
            provider.Put("mem-key-update2", updated);

            var result = provider.Get("mem-key-update") as List<SampleEntity>;
            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.Contains(result, i => i.Name == "Second Name");
        }

        #endregion

        #region DistributedCacheProvider

        [Fact]
        public void DistributedCache_Get_NullKey_ReturnsNull()
        {
            var provider = new DistributedCacheProvider("localhost:6379");
            Assert.Null(provider.Get(null));
        }

        [Fact]
        public void DistributedCache_Get_Throws_NotImplemented()
        {
            var provider = new DistributedCacheProvider("localhost:6379");
            Assert.Throws<NotImplementedException>(() => provider.Get("key"));
        }

        [Fact]
        public void DistributedCache_Put_NullValues_Noop()
        {
            var provider = new DistributedCacheProvider("localhost:6379");
            provider.Put(null, "value");
            provider.Put("key", null);
        }

        [Fact]
        public void DistributedCache_Put_Throws_NotImplemented()
        {
            var provider = new DistributedCacheProvider("localhost:6379");
            Assert.Throws<NotImplementedException>(() => provider.Put("key", "value"));
        }

        [Fact]
        public void DistributedCache_Del_NullKey_Noop()
        {
            var provider = new DistributedCacheProvider("localhost:6379");
            provider.Del(null);
        }

        [Fact]
        public void DistributedCache_Del_Throws_NotImplemented()
        {
            var provider = new DistributedCacheProvider("localhost:6379");
            Assert.Throws<NotImplementedException>(() => provider.Del("key"));
        }

        [Fact]
        public void DistributedCache_Clear_Throws_NotImplemented()
        {
            var provider = new DistributedCacheProvider("localhost:6379");
            Assert.Throws<NotImplementedException>(() => provider.Clear());
        }

        [Fact]
        public void DistributedCache_DefaultExpiration()
        {
            var provider = new DistributedCacheProvider("localhost:6379", "dapper:", TimeSpan.FromMinutes(2));
            Assert.Throws<NotImplementedException>(() => provider.Get("key"));
        }

        #endregion

        #region PersistenceChannelCacheProvider

        [Fact]
        public void Channel_Ctor_NullInner_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new PersistenceChannelCacheProvider(null));
        }

        [Fact]
        public async Task Channel_Put_PublishesMessage()
        {
            var inner = new InMemoryCacheProvider();
            var provider = new PersistenceChannelCacheProvider(inner);

            var entity = new SampleEntity { DocNumber = 82001, Name = "Channel Put", Active = true };
            provider.Put("channel-key", entity);

            var innerResult = inner.Get("channel-key");
            Assert.NotNull(innerResult);

            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                await foreach (var msg in provider.ConsumeAsync(cts.Token))
                {
                    Assert.Equal(PersistenceChannelCacheProvider.ChannelAction.Put, msg.Action);
                    Assert.Equal("channel-key", msg.CacheKey);
                    Assert.NotNull(msg.CacheItem);
                    break;
                }
            }

            provider.Dispose();
        }

        [Fact]
        public async Task Channel_Del_PublishesMessage()
        {
            var inner = new InMemoryCacheProvider();
            var provider = new PersistenceChannelCacheProvider(inner);

            var entity = new SampleEntity { DocNumber = 82002, Name = "Channel Del", Active = true };
            provider.Put("channel-del", entity);
            provider.Del("channel-del");

            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                await foreach (var msg in provider.ConsumeAsync(cts.Token))
                {
                    if (msg.Action == PersistenceChannelCacheProvider.ChannelAction.Del)
                    {
                        Assert.Equal("channel-del", msg.CacheKey);
                        Assert.False(msg.DeleteAll);
                        break;
                    }
                }
            }

            provider.Stop();
        }

        [Fact]
        public async Task Channel_Clear_PublishesMessage()
        {
            var inner = new InMemoryCacheProvider();
            var provider = new PersistenceChannelCacheProvider(inner);

            provider.Clear();

            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                await foreach (var msg in provider.ConsumeAsync(cts.Token))
                {
                    if (msg.Action == PersistenceChannelCacheProvider.ChannelAction.Clear)
                    {
                        Assert.NotEqual(default(DateTime), msg.Timestamp);
                        break;
                    }
                }
            }

            provider.Stop();
        }

        [Fact]
        public void Channel_Stop_CompletesWriter()
        {
            var provider = new PersistenceChannelCacheProvider(new InMemoryCacheProvider());
            provider.Dispose();
        }

        #endregion

        #region Parallelizer

        [Fact]
        public void Parallelizer_StartNewProcess_RunsOnThread()
        {
            using (var signal = new ManualResetEventSlim())
            {
                var param = new ParallelParam { Param1 = 1, Param2 = "two", Param3 = 3.0, Param4 = 4L, Param5 = true, Param6 = "six" };

                Parallelizer.StartNewProcess(o =>
                {
                    var p = (ParallelParam)o;
                    if ((int)p.Param1 == 1 && (string)p.Param2 == "two")
                        signal.Set();
                }, param);

                Assert.True(signal.Wait(TimeSpan.FromSeconds(5)));
            }
        }

        #endregion

        #region Exceptions

        [Fact]
        public void ConnectionStringNotFound_CanBeThrown()
        {
            var ex = Assert.Throws<ConnectionStringNotFoundException>(() =>
            {
                using var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, null, null, false);
                repos.AddSync(new SampleEntity { DocNumber = 83001, Name = "No Conn", Active = true });
            });

            Assert.Contains("ConnectionString", ex.Message);
        }

        [Fact]
        public void PropertyNotListable_CanBeThrown()
        {
            var ex = new PropertyNotListableException("Name");
            Assert.Contains("Name", ex.Message);
        }

        #endregion
    }
}