using System.Collections.Generic;
using System.Threading.Tasks;
using Rochas.DapperRepository.Specification.Enums;

namespace Rochas.DapperRepository.Specification.Interfaces
{
    public interface IQueryBuilder<T> where T : class
    {
        IQueryBuilder<T> OrderBy(string sortAttribute, bool descending = false);
        IQueryBuilder<T> OrderBy(string[] sortAttributes, bool descending = false);
        IQueryBuilder<T> GroupBy(string groupAttribute);
        IQueryBuilder<T> GroupBy(string[] groupAttributes);
        IQueryBuilder<T> Descending();
        Task<ICollection<T>> ToListAsync();
        ICollection<T> ToList();
    }
}
