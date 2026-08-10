using System.Collections.Generic;
using System.Threading.Tasks;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Specification.Models;

namespace Rochas.DapperRepository.Specification.Interfaces
{
    public interface IGenericRepository<T> where T : class
    {
        void Initialize(string databaseFileName, string tableScript);

        // ── PERSISTÊNCIA ─────────────────────────────────────────────

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

        // ── GET ──────────────────────────────────────────────────────

        Task<T> Get(object key, bool loadComposition = false);
        T GetSync(object key, bool loadComposition = false);

        Task<T> Get(T filter, bool loadComposition = false);
        T GetSync(T filter, bool loadComposition = false);

        // ── SEARCH (builder) ─────────────────────────────────────────

        IQueryBuilder<T> Search(object criteria, bool loadComposition = false, bool filterConjunction = false);
        IQuerySyncBuilder<T> SearchSync(object criteria, bool loadComposition = false, bool filterConjunction = false);
        IQueryPaginatedBuilder<T> Search(object criteria, int page, int pageSize, bool loadComposition = false, bool filterConjunction = false);
        IQueryPaginatedBuilder<T> SearchSync(object criteria, int page, int pageSize, bool loadComposition = false, bool filterConjunction = false);

        // ── BULK SEARCH ─────────────────────────────────────────────

        ICollection<T> BulkSearch(object[] criterias, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        ICollection<T> BulkSearchSync(object[] criterias, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);

        // ── QUERY (builder) ──────────────────────────────────────────

        IQueryBuilder<T> Query(T filter, bool loadComposition = false, bool filterConjunction = false);
        IQueryPaginatedBuilder<T> Query(T filter, int page, int pageSize, bool loadComposition = false, bool filterConjunction = false);
        IQueryBuilder<T> OrderBy(params string[] sortAttributes);
        IQueryBuilder<T> OrderByDescending(params string[] sortAttributes);
        IQueryBuilder<T> GroupBy(string[] groupAttributes, Dictionary<string, DataAggregationType> aggregates = null);

        // ── QUERY (sync builder) ─────────────────────────────────────

        IQuerySyncBuilder<T> QuerySync(T filter, bool loadComposition = false, bool filterConjunction = false);
        IQueryPaginatedBuilder<T> QuerySync(T filter, int page, int pageSize, bool loadComposition = false, bool filterConjunction = false);

        // ── QUERY RAW ────────────────────────────────────────────────

        Task<ICollection<T>> QueryRaw(string sql, Dictionary<string, object> parameters);
        ICollection<T> QueryRawSync(string sql, Dictionary<string, object> parameters);

        Task<PaginatedResult<T>> QueryRaw(string sql, string countSql, Dictionary<string, object> parameters, int page = 1, int pageSize = 20);
        PaginatedResult<T> QueryRawSync(string sql, string countSql, Dictionary<string, object> parameters, int page = 1, int pageSize = 20);

        void Dispose();
    }
}
