using Rochas.BWOQ.Data;
using Rochas.Data.Specification.Interfaces;

namespace Rochas.DapperRepository
{
    /// <summary>
    /// Concrete IBwoqQueryBuilder backed by the BWOQ interpreter: Q/W/G/O/OD
    /// expressions are accumulated and resolved, at ToQuery time, into the
    /// existing execution builders through BwoqRepositoryQuery.
    /// </summary>
    public sealed class BwoqQueryBuilder<T> : IBwoqQueryBuilder<T> where T : class
    {
        private readonly GenericRepository<T> _repository;
        private readonly BwoqQuery<T> _query = BwoqQuery<T>.Create();

        public BwoqQueryBuilder(GenericRepository<T> repository)
        {
            _repository = repository ?? throw new System.ArgumentNullException(nameof(repository));
        }

        public IBwoqQueryBuilder<T> Q(string selectExpression)
        {
            _query.Select(selectExpression);
            return this;
        }

        public IBwoqQueryBuilder<T> W(string whereExpression)
        {
            _query.Where(whereExpression);
            return this;
        }

        public IBwoqQueryBuilder<T> G(string groupExpression, string byExpression)
        {
            _query.GroupBy(groupExpression, byExpression);
            return this;
        }

        public IBwoqQueryBuilder<T> O(string orderExpression)
        {
            _query.OrderBy(orderExpression);
            return this;
        }

        public IBwoqQueryBuilder<T> OD(string orderExpression)
        {
            _query.OrderByDescending(orderExpression);
            return this;
        }

        public IQueryBuilder<T> ToQuery()
        {
            return _query.ToRepositoryQuery().Build(_repository);
        }

        public IQuerySyncBuilder<T> ToQuerySync()
        {
            return _query.ToRepositoryQuery().BuildSync(_repository);
        }

        public IQueryPaginatedBuilder<T> ToQuery(int page, int pageSize)
        {
            return _query.ToRepositoryQuery().Build(_repository, page, pageSize);
        }
    }
}