using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Rochas.DapperRepository.Specification.Enums;
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
        private string _groupAttributes;
        private Dictionary<string, DataAggregationType> _aggregates;
        private bool _orderDescending;
        private readonly int _page;
        private readonly int _pageSize;

        public QueryPaginatedBuilder(IGenericRepository<T> repository, T filter, bool loadComposition = false,
            bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false,
            string groupAttributes = null, Dictionary<string, DataAggregationType> aggregates = null,
            int page = 1, int pageSize = 20)
        {
            _repository = repository;
            _filter = filter;
            _loadComposition = loadComposition;
            _filterConjunction = filterConjunction;
            _sortAttributes = sortAttributes;
            _orderDescending = orderDescending;
            _groupAttributes = groupAttributes;
            _aggregates = aggregates;
            _page = Math.Max(1, page);
            _pageSize = Math.Max(1, pageSize);
        }

        public IQueryPaginatedBuilder<T> OrderBy(params string[] sortAttributes)
        {
            _sortAttributes = string.Join(",", sortAttributes);
            _orderDescending = false;
            return this;
        }

        public IQueryPaginatedBuilder<T> OrderByDescending(params string[] sortAttributes)
        {
            _sortAttributes = string.Join(",", sortAttributes);
            _orderDescending = true;
            return this;
        }

        public IQueryPaginatedBuilder<T> GroupBy(params string[] groupAttributes)
        {
            _groupAttributes = string.Join(",", groupAttributes);
            return this;
        }

        public IQueryPaginatedBuilder<T> GroupBy(string[] groupAttributes, Dictionary<string, DataAggregationType> aggregates)
        {
            _groupAttributes = string.Join(",", groupAttributes);
            _aggregates = aggregates;
            return this;
        }

        public PaginatedResult<T> Paginate(int page, int pageSize)
        {
            var p = Math.Max(1, page);
            var ps = Math.Max(1, pageSize);
            var repo = (GenericRepository<T>)_repository;

            if (_aggregates != null && _groupAttributes != null)
            {
                return repo.QueryPaginatedWithBuilder(
                    _filter, p, ps, _loadComposition, _filterConjunction,
                    _sortAttributes, _orderDescending, _groupAttributes, _aggregates)
                    .GetAwaiter().GetResult();
            }

            return repo.QueryPaginatedWithBuilder(
                _filter, p, ps, _loadComposition, _filterConjunction,
                _sortAttributes, _orderDescending, _groupAttributes)
                .GetAwaiter().GetResult();
        }

        public PaginatedResult<T> ToList()
        {
            return Paginate(_page, _pageSize);
        }

        public TaskAwaiter<PaginatedResult<T>> GetAwaiter()
        {
            return ToListAsync().GetAwaiter();
        }

        private Task<PaginatedResult<T>> ToListAsync()
        {
            return Task.FromResult(Paginate(_page, _pageSize));
        }
    }
}
