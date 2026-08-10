using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Specification.Models;

namespace Rochas.DapperRepository.Specification.Interfaces
{
    public interface IQueryPaginatedBuilder<T> where T : class
    {
        IQueryPaginatedBuilder<T> OrderBy(params string[] sortAttributes);
        IQueryPaginatedBuilder<T> OrderByDescending(params string[] sortAttributes);
        IQueryPaginatedBuilder<T> GroupBy(params string[] groupAttributes);
        IQueryPaginatedBuilder<T> GroupBy(string[] groupAttributes, Dictionary<string, DataAggregationType> aggregates);
        TaskAwaiter<PaginatedResult<T>> GetAwaiter();
        PaginatedResult<T> ToList();
    }
}
