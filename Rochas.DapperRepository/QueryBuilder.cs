using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Rochas.DapperRepository.Specification.Interfaces;

namespace Rochas.DapperRepository
{
	public class QueryBuilder<T> : IQueryBuilder<T> where T : class
	{
		private readonly IGenericRepository<T> _repository;
		private readonly T _filter;
		private readonly bool _loadComposition;
		private readonly bool _filterConjunction;
		private string _sortAttributes;
		private string _groupAttributes;
		private bool _orderDescending;

		public QueryBuilder(IGenericRepository<T> repository, T filter, bool loadComposition = false, bool filterConjunction = false)
		{
			_repository = repository;
			_filter = filter;
			_loadComposition = loadComposition;
			_filterConjunction = filterConjunction;
		}

		public IQueryBuilder<T> OrderBy(string[] sortAttributes, bool descending = false)
		{
			_sortAttributes = string.Join(",", sortAttributes);
			_orderDescending = descending;
			return this;
		}

		public IQueryBuilder<T> GroupBy(string[] groupAttributes)
		{
			_groupAttributes = string.Join(",", groupAttributes);
			return this;
		}

		public IQueryBuilder<T> Descending()
		{
			_orderDescending = true;
			return this;
		}

		public TaskAwaiter<ICollection<T>> GetAwaiter()
		{
			return ExecuteAsync().GetAwaiter();
		}

		public async Task<ICollection<T>> ToListAsync()
		{
			return await ExecuteAsync();
		}

		public ICollection<T> ToList()
		{
			return ExecuteAsync().GetAwaiter().GetResult();
		}

		private async Task<ICollection<T>> ExecuteAsync()
		{
			return await _repository.QueryWithBuilder(
				_filter,
				_loadComposition,
				_filterConjunction,
				_sortAttributes,
				_orderDescending,
				_groupAttributes);
		}
	}
}
