using System;
using System.Threading.Tasks;
using Rochas.DapperRepository.Specification.Interfaces;
using Rochas.DapperRepository.Specification.Models;

namespace Rochas.DapperRepository
{
	public class QueryPaginatedBuilder<T> : IQueryPaginatedBuilder<T> where T : class
	{
		private readonly IGenericRepository<T> _repository;
		private readonly T _filter;
		private readonly bool _loadComposition;
		private readonly bool _filterConjunction;
		private string _sortAttributes;
		private bool _orderDescending;

		public QueryPaginatedBuilder(IGenericRepository<T> repository, T filter, bool loadComposition = false, bool filterConjunction = false)
		{
			_repository = repository;
			_filter = filter;
			_loadComposition = loadComposition;
			_filterConjunction = filterConjunction;
		}

		public IQueryPaginatedBuilder<T> OrderBy(string sortAttribute, bool descending = false)
		{
			_sortAttributes = sortAttribute;
			_orderDescending = descending;
			return this;
		}

		public IQueryPaginatedBuilder<T> OrderBy(string[] sortAttributes, bool descending = false)
		{
			_sortAttributes = string.Join(",", sortAttributes);
			_orderDescending = descending;
			return this;
		}

		public IQueryPaginatedBuilder<T> Descending()
		{
			_orderDescending = true;
			return this;
		}

		public async Task<PaginatedResult<T>> PaginateAsync(int page = 1, int pageSize = 20)
		{
			return await _repository.QueryPaginatedWithBuilder(
				_filter,
				Math.Max(1, page),
				Math.Max(1, pageSize),
				_loadComposition,
				_filterConjunction,
				_sortAttributes,
				_orderDescending);
		}

		public PaginatedResult<T> Paginate(int page = 1, int pageSize = 20)
		{
			return PaginateAsync(page, pageSize).GetAwaiter().GetResult();
		}
	}
}
