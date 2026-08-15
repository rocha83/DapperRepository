using System.Collections.Generic;
using Rochas.Data.Specification.Enums;

namespace Rochas.Data.Specification.Interfaces
{
    public interface IQuerySyncBuilder<T> where T : class
    {
        IQuerySyncBuilder<T> OrderBy(params string[] sortAttributes);
        IQuerySyncBuilder<T> OrderByDescending(params string[] sortAttributes);
        IQuerySyncBuilder<T> GroupBy(params string[] groupAttributes);
        IQuerySyncBuilder<T> GroupBy(string[] groupAttributes, Dictionary<string, DataAggregationType> aggregates);
        IQueryPaginatedBuilder<T> Paginate(int page = 1, int pageSize = 20);
        int Count();
        ICollection<T> ToList();
    }
}
