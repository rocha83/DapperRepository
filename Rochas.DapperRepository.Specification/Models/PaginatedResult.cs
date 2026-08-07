using System.Collections.Generic;

namespace Rochas.DapperRepository.Specification.Models
{
    public class PaginatedResult<T>
    {
        public IReadOnlyList<T> Items { get; set; }
        public int TotalCount { get; set; }
        public int Page { get; set; }
        public int PageSize { get; set; }
        public int PageCount => PageSize > 0 ? (int)System.Math.Ceiling((double)TotalCount / PageSize) : 0;

        public PaginatedResult()
        {
            Items = new List<T>();
        }

        public PaginatedResult(IReadOnlyList<T> items, int totalCount, int page, int pageSize)
        {
            Items = items;
            TotalCount = totalCount;
            Page = page;
            PageSize = pageSize;
        }
    }
}
