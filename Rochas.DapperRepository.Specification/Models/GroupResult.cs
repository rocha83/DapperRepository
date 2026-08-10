using System.Collections.Generic;
using Rochas.DapperRepository.Specification.Enums;

namespace Rochas.DapperRepository.Specification.Models
{
    public class GroupResult<TKey, TItem> where TItem : class
    {
        public TKey Key { get; set; }
        public Dictionary<decimal, DataAggregationType> Aggregates { get; set; }
        public ICollection<TItem> Items { get; set; }

        public GroupResult()
        {
            Items = new List<TItem>();
        }

        public GroupResult(TKey key, ICollection<TItem> items, Dictionary<decimal, DataAggregationType> aggregates = null)
        {
            Key = key;
            Items = items;
            Aggregates = aggregates;
        }
    }
}
