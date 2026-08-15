using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Rochas.Data.Specification.Enums;
using Rochas.Data.Specification.Interfaces;
using Rochas.Data.Specification.Models;

namespace Rochas.DapperRepository.Builders
{
	public class QueryBuilder<T> : IQueryBuilder<T> where T : class
	{
		private readonly IGenericRepository<T> _repository;
		private readonly T _filter;
		private readonly bool _loadComposition;
		private readonly bool _filterConjunction;
		private string _sortAttributes;
		private string _groupAttributes;
		private Dictionary<string, DataAggregationType> _aggregates;
		private bool _orderDescending;

		public QueryBuilder(IGenericRepository<T> repository, T filter, bool loadComposition = false, bool filterConjunction = false)
		{
			_repository = repository;
			_filter = filter;
			_loadComposition = loadComposition;
			_filterConjunction = filterConjunction;
		}

		public IQueryBuilder<T> OrderBy(params string[] sortAttributes)
		{
			_sortAttributes = string.Join(",", sortAttributes);
			_orderDescending = false;
			return this;
		}

		public IQueryBuilder<T> GroupBy(params string[] groupAttributes)
		{
			_groupAttributes = string.Join(",", groupAttributes);
			return this;
		}

		public IQueryBuilder<T> GroupBy(string[] groupAttributes, Dictionary<string, DataAggregationType> aggregates)
		{
			_groupAttributes = string.Join(",", groupAttributes);
			_aggregates = aggregates;
			return this;
		}

		public IQueryBuilder<T> OrderByDescending(params string[] sortAttributes)
		{
			_sortAttributes = string.Join(",", sortAttributes);
			_orderDescending = true;
			return this;
		}

		public IQueryPaginatedBuilder<T> Paginate(int page = 1, int pageSize = 20)
		{
			return new QueryPaginatedBuilder<T>(_repository, _filter, _loadComposition, _filterConjunction,
				_sortAttributes, _orderDescending, _groupAttributes, _aggregates, page, pageSize);
		}

		public TaskAwaiter<ICollection<T>> GetAwaiter()
		{
			return ExecuteAsync().GetAwaiter();
		}

		private async Task<ICollection<T>> ExecuteAsync()
		{
			var repo = (GenericRepository<T>)_repository;

			if (_aggregates != null && _groupAttributes != null)
			{
				return await repo.QueryWithBuilder(
					_filter,
					_loadComposition,
					_filterConjunction,
					_sortAttributes,
					_orderDescending,
					_groupAttributes,
					_aggregates);
			}

			return await repo.QueryWithBuilder(
				_filter,
				_loadComposition,
				_filterConjunction,
				_sortAttributes,
				_orderDescending,
				_groupAttributes);
		}
	}
}
