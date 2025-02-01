using System.Collections.Generic;
using System.Threading.Tasks;

namespace Rochas.DapperRepository.Specification.Interfaces
{
    public interface IGenericRepository<T> where T : class
    {
        void Initialize(string databaseFileName, string tableScript);
        Task<int> Count(T filterEntity);
        int CountSync(T filterEntity);
        Task<int> Create(T entity, bool persistComposition = false);
        int CreateSync(T entity, bool persistComposition = false);
        Task CreateRange(IEnumerable<T> entities, bool persistComposition = false);
        void CreateRangeSync(IEnumerable<T> entities, bool persistComposition = false);
        Task<int> Delete(T filterEntity);
        int DeleteSync(T filterEntity);
        Task<int> Edit(T entity, T filterEntity, bool persistComposition = false);
        int EditSync(T entity, T filterEntity, bool persistComposition = false);
        Task<T> Get(object key, bool loadComposition = false);
        T GetSync(object key, bool loadComposition = false);
        Task<T> Get(T filter, bool loadComposition = false);
        T GetSync(T filter, bool loadComposition = false);
        Task<ICollection<T>> Search(object criteria, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        ICollection<T> SearchSync(object criteria, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false);
        Task<ICollection<T>> List(T filter, bool loadComposition = false, int recordsLimit = 0, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false);
        ICollection<T> ListSync(T filter, bool loadComposition = false, int recordsLimit = 0, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false);

        void Dispose();
    }
}