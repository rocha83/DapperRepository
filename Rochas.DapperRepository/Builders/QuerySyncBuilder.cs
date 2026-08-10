using System.Collections.Generic;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Specification.Interfaces;

namespace Rochas.DapperRepository.Builders
{
    public class QuerySyncBuilder<T> : IQuerySyncBuilder<T> where T : class
    {
        private readonly IGenericRepository<T> _repository;
        private readonly T _filter;
        private readonly bool _loadComposition;
        private readonly bool _filterConjunction;
        private string _sortAttributes;
        private string _groupAttributes;
        private Dictionary<string, DataAggregationType> _aggregates;
        private bool _orderDescending;

        public QuerySyncBuilder(IGenericRepository<T> repository, T filter, bool loadComposition = false, bool filterConjunction = false)
        {
            _repository = repository;
            _filter = filter;
            _loadComposition = loadComposition;
            _filterConjunction = filterConjunction;
        }

        public IQuerySyncBuilder<T> OrderBy(params string[] sortAttributes)
        {
            _sortAttributes = string.Join(",", sortAttributes);
            _orderDescending = false;
            return this;
        }

        public IQuerySyncBuilder<T> OrderByDescending(params string[] sortAttributes)
        {
            _sortAttributes = string.Join(",", sortAttributes);
            _orderDescending = true;
            return this;
        }

        public IQuerySyncBuilder<T> GroupBy(params string[] groupAttributes)
        {
            _groupAttributes = string.Join(",", groupAttributes);
            return this;
        }

        public IQuerySyncBuilder<T> GroupBy(string[] groupAttributes, Dictionary<string, DataAggregationType> aggregates)
        {
            _groupAttributes = string.Join(",", groupAttributes);
            _aggregates = aggregates;
            return this;
        }

        public IQueryPaginatedBuilder<T> Paginate(int page = 1, int pageSize = 20)
        {
            return new QueryPaginatedBuilder<T>(_repository, _filter, _loadComposition, _filterConjunction,
                _sortAttributes, _orderDescending, _groupAttributes, _aggregates, page, pageSize);
        }

        public int Count()
        {
            return ((GenericRepository<T>)_repository).CountWithBuilder(
                _filter, _loadComposition, _filterConjunction,
                _sortAttributes, _orderDescending, _groupAttributes, _aggregates);
        }

        public ICollection<T> ToList()
        {
            return ((GenericRepository<T>)_repository).QueryWithBuilder(
                _filter, _loadComposition, _filterConjunction,
                _sortAttributes, _orderDescending, _groupAttributes, _aggregates)
                .GetAwaiter().GetResult();
        }
    }
}
