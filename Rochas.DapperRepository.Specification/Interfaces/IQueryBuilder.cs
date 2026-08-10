using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Specification.Models;

namespace Rochas.DapperRepository.Specification.Interfaces
{
    public interface IQueryBuilder<T> where T : class
    {
        IQueryBuilder<T> OrderBy(params string[] sortAttributes);
        IQueryBuilder<T> OrderByDescending(params string[] sortAttributes);
        IQueryBuilder<T> GroupBy(params string[] groupAttributes);
        IQueryBuilder<T> GroupBy(string[] groupAttributes, Dictionary<string, DataAggregationType> aggregates);
        IQueryPaginatedBuilder<T> Paginate(int page = 1, int pageSize = 20);
        TaskAwaiter<ICollection<T>> GetAwaiter();
    }
}
