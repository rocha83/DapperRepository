using System.Collections.Generic;
using System.Threading.Tasks;
using Rochas.DapperRepository.Specification.Models;

namespace Rochas.DapperRepository.Specification.Interfaces
{
    public interface IGenericRepository<T> where T : class
    {
        void Initialize(string databaseFileName, string tableScript);
        Task<int> Count(T filterEntity);
        int CountSync(T filterEntity);
        Task<int> Add(T entity, bool persistComposition = false);
        int AddSync(T entity, bool persistComposition = false);
        Task AddRange(IEnumerable<T> entities, bool persistComposition = false);
        void AddRangeSync(IEnumerable<T> entities, bool persistComposition = false);
        Task<int> Remove(T filterEntity);
        int RemoveSync(T filterEntity);
        Task<int> Update(T entity, T filterEntity, bool persistComposition = false);
        int UpdateSync(T entity, T filterEntity, bool persistComposition = false);
        Task<T> Get(object key, bool loadComposition = false);
        T GetSync(object key, bool loadComposition = false);
        Task<T> Get(T filter, bool loadComposition = false);
        T GetSync(T filter, bool loadComposition = false);
        Task<ICollection<T>> Search(object criteria, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        ICollection<T> SearchSync(object criteria, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        ICollection<T> BulkSearch(object[] criterias, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        ICollection<T> BulkSearchSync(object[] criterias, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        Task<ICollection<T>> Query(T filter, bool loadComposition = false, int recordsLimit = 0, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false, string groupAttributes = null);
        ICollection<T> QuerySync(T filter, bool loadComposition = false, int recordsLimit = 0, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false, string groupAttributes = null);

        Task<PaginatedResult<T>> SearchPaginated(object criteria, int page = 1, int pageSize = 20, bool loadComposition = false, string sortAttributes = null, bool orderDescending = false);
        PaginatedResult<T> SearchPaginatedSync(object criteria, int page = 1, int pageSize = 20, bool loadComposition = false, string sortAttributes = null, bool orderDescending = false);
        Task<PaginatedResult<T>> QueryPaginated(T filter, int page = 1, int pageSize = 20, bool loadComposition = false, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false);
        PaginatedResult<T> QueryPaginatedSync(T filter, int page = 1, int pageSize = 20, bool loadComposition = false, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false);

        Task<ICollection<T>> QueryWithBuilder(T filter, bool loadComposition, bool filterConjunction, string sortAttributes, bool orderDescending, string groupAttributes);
        Task<PaginatedResult<T>> QueryPaginatedWithBuilder(T filter, int page, int pageSize, bool loadComposition, bool filterConjunction, string sortAttributes, bool orderDescending);

        IQueryBuilder<T> Query(T filter, bool loadComposition = false, bool filterConjunction = false);
        IQueryPaginatedBuilder<T> QueryPaginated(T filter, bool loadComposition = false, bool filterConjunction = false);

        void Dispose();
    }
}
