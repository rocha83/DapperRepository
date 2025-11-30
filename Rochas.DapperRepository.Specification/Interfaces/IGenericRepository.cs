using System.Collections.Generic;
using System.Threading.Tasks;

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
        Task<ICollection<T>> Query(T filter, bool loadComposition = false, int recordsLimit = 0, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false);
        ICollection<T> QuerySync(T filter, bool loadComposition = false, int recordsLimit = 0, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false);

        void Dispose();
    }
}