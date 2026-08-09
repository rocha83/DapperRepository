using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Rochas.DapperRepository.Specification.Interfaces
{
    public interface IQueryBuilder<T> where T : class
    {
        IQueryBuilder<T> OrderBy(string[] sortAttributes, bool descending = false);
        IQueryBuilder<T> GroupBy(string[] groupAttributes);
        IQueryBuilder<T> Descending();
        TaskAwaiter<ICollection<T>> GetAwaiter();
        Task<ICollection<T>> ToListAsync();
        ICollection<T> ToList();
    }
}
