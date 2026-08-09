using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Rochas.DapperRepository.Base;
using Rochas.DapperRepository.Specification.Enums;

namespace Rochas.DapperRepository
{
	public class QueryBuilder<T> where T : class
	{
		#region Declarations

		private readonly GenericRepository<T> _repository;
		private readonly T _filter;
		private readonly bool _loadComposition;
		private readonly bool _filterConjunction;
		private string _sortAttributes;
		private string _groupAttributes;
		private bool _orderDescending;

		#endregion

		#region Constructors

		internal QueryBuilder(GenericRepository<T> repository, T filter, bool loadComposition = false, bool filterConjunction = false)
		{
			_repository = repository;
			_filter = filter;
			_loadComposition = loadComposition;
			_filterConjunction = filterConjunction;
		}

		#endregion

		#region Builder Methods

		public QueryBuilder<T> OrderBy(string sortAttribute, bool descending = false)
		{
			_sortAttributes = sortAttribute;
			_orderDescending = descending;
			return this;
		}

		public QueryBuilder<T> OrderBy(string[] sortAttributes, bool descending = false)
		{
			_sortAttributes = string.Join(",", sortAttributes);
			_orderDescending = descending;
			return this;
		}

		public QueryBuilder<T> GroupBy(string groupAttribute)
		{
			_groupAttributes = groupAttribute;
			return this;
		}

		public QueryBuilder<T> GroupBy(string[] groupAttributes)
		{
			_groupAttributes = string.Join(",", groupAttributes);
			return this;
		}

		public QueryBuilder<T> Descending()
		{
			_orderDescending = true;
			return this;
		}

		#endregion

		#region Terminal Methods

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

		#endregion

		#region Private Methods

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

		#endregion
	}
}
