using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace Rochas.DapperRepository.Helpers
{
    public static class DataCache
    {
        #region Declarations

        private static ConcurrentDictionary<KeyValuePair<uint, string>, object> cacheItems = 
            new ConcurrentDictionary<KeyValuePair<uint, string>, object>();

        public static int MemorySizeLimit;

        #endregion

        #region Public Methods

        public static void Initialize(int memorySizeLimit)
        {
            MemorySizeLimit = memorySizeLimit;
        }

        public static object Get(object cacheKey)
        {
            object result = null;

            if (cacheKey != null)
            {
                var serialKey = JsonSerializer.Serialize(cacheKey);
                var serialCacheKey = new KeyValuePair<uint, string>(GetCustomHashCode(cacheKey.GetType().FullName), serialKey);
                if (cacheItems.ContainsKey(serialCacheKey))
                    result = cacheItems[serialCacheKey];
            }

            var listResult = result as IList;

            if ((listResult != null) && (listResult.Count == 1))
                result = ((IList)result)[0];
            
            return result;
        }

        public static void Put(object cacheKey, object cacheItem)
        {
            try
            {
                if ((cacheKey != null) && (cacheItem != null))
                {
                    CheckMemoryUsage();

                    var serialKey = JsonSerializer.Serialize(cacheKey);
                    var serialCacheKey = new KeyValuePair<uint, string>(GetCustomHashCode(cacheKey.GetType().FullName), serialKey);

                    if (!cacheItems.ContainsKey(serialCacheKey))
                        cacheItems.TryAdd(serialCacheKey, cacheItem);

                    UpdateCacheTree(cacheKey.GetType().GetHashCode(), cacheItem);
                }
            }
            catch (Exception ex)
            {
                throw ex; 
            }
        }

        public static void Del(object cacheKey, bool deleteAll = false)
        {
            if (cacheKey != null)
            {
                var serialKey = JsonSerializer.Serialize(cacheKey);
                var serialCacheKey = new KeyValuePair<uint, string>(GetCustomHashCode(cacheKey.GetType().FullName), serialKey);
                if (cacheItems.ContainsKey(serialCacheKey))
                    cacheItems.TryRemove(serialCacheKey, out var _fake);

                if (deleteAll)
                    UpdateCacheTree(cacheKey.GetType().GetHashCode(), cacheKey, true);
            }
        }

        public static void Clear()
        {
            cacheItems = null;
        }

        #endregion

        #region Helper Methods

        private static void CheckMemoryUsage()
        {
            // Verificando limite do cache

            if (MemorySizeLimit > 0)
            {
                var paramSize = MemorySizeLimit;
                var memSize = GC.GetTotalMemory(false) / 1024 / 1024;
                if (memSize > paramSize)
                    cacheItems = new ConcurrentDictionary<KeyValuePair<uint, string>, object>();
            }
        }

        private static void UpdateCacheTree(int typeKeyCode, object cacheItem, bool removeItem = false)
        {
            if (!(cacheItem is IList))
            {
                var typeCacheItems = cacheItems.Where(itm => itm.Key.Key.Equals(typeKeyCode)).ToList();
                var itemProps = cacheItem.GetType().GetProperties();
                var itemKeyId = EntityReflector.GetKeyColumn(itemProps).GetValue(cacheItem, null);

                for (var typeCount = 0; typeCount < typeCacheItems.Count; typeCount++)
                {
                    if (!removeItem)
                    {
                        var listValue = typeCacheItems[typeCount].Value as IList;

                        if (listValue != null)
                            for (int valueCount = 0; valueCount < ((IList)typeCacheItems[typeCount].Value).Count; valueCount++)
                            {
                                if (EntityReflector.MatchKeys(cacheItem, itemProps, listValue[valueCount]))
                                {
                                    listValue[valueCount] = cacheItem;
                                }
                            }
                    }
                    else
                        cacheItems.TryRemove(typeCacheItems[typeCount].Key, out var _fake);
                }
            }
        }

        private static uint GetCustomHashCode(string value)
        {
            if (string.IsNullOrEmpty(value))
                return 0;

            const uint prime = 16777619;
            uint hash = 2166136261;

            foreach (char c in value)
            {
                hash ^= (byte)c;
                hash *= prime;
            }

            return hash;
        }

        #endregion
    }
}
