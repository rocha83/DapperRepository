using System.Threading.Tasks;
using Rochas.DapperRepository.Specification.Models;

namespace Rochas.DapperRepository.Specification.Interfaces
{
    public interface IQueryPaginatedBuilder<T> where T : class
    {
        IQueryPaginatedBuilder<T> OrderBy(string sortAttribute, bool descending = false);
        IQueryPaginatedBuilder<T> OrderBy(string[] sortAttributes, bool descending = false);
        IQueryPaginatedBuilder<T> Descending();
        Task<PaginatedResult<T>> PaginateAsync(int page = 1, int pageSize = 20);
        PaginatedResult<T> Paginate(int page = 1, int pageSize = 20);
    }
}
