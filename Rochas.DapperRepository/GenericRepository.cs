using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text.Json;
using System.Reflection;
using System.Data;
using System.Collections.Concurrent;
using Rochas.DapperRepository.Base;
using Rochas.DapperRepository.Helpers;
using Rochas.SqlWrapper.Helpers;
using Rochas.Data.Specification.Models;
using Rochas.Data.Specification.Enums;
using Rochas.Data.Specification.Interfaces;
using Rochas.Data.Specification.Annotations;
using Rochas.DapperRepository.Builders;

namespace Rochas.DapperRepository
{
	/// <summary>
	/// Full generic repository: write persistence + read (Get/GetSync) +
	/// query builders (Search/Query/OrderBy/GroupBy/Count/QueryRaw) + BWOQ grammar.
	/// Inherits PersistenceRepository for all write/read internals.
	/// </summary>
	public class GenericRepository<T> : PersistenceRepository<T>, IGenericRepository<T>, IGenericBwoqRepository<T> where T : class
	{
		#region Constructors

		public GenericRepository(DatabaseEngine engine, string connectionString, string logPath = null,
			bool keepConnected = false, bool readUncommied = false, bool forceSnakeCase = false,
			ICacheProvider cacheProvider = null, params string[] replicaConnStrings)
			: base(engine, connectionString, logPath, keepConnected, readUncommied, forceSnakeCase, cacheProvider, replicaConnStrings)
		{
		}

		public GenericRepository(string connectionString, string logPath = null,
			bool keepConnected = false, bool readUncommied = false, bool forceSnakeCase = false,
			ICacheProvider cacheProvider = null, params string[] replicaConnStrings)
			: base(connectionString, logPath, keepConnected, readUncommied, forceSnakeCase, cacheProvider, replicaConnStrings)
		{
		}

		public GenericRepository(IDbConnection dbConnection, string logPath = null,
			bool keepConnected = false, bool readUncommied = false, bool forceSnakeCase = false,
			ICacheProvider cacheProvider = null, params string[] replicaConnStrings)
			: base(dbConnection, logPath, keepConnected, readUncommied, forceSnakeCase, cacheProvider, replicaConnStrings)
		{
		}

		#endregion

		#region Query Builders — Search

		public IQueryBuilder<T> Search(object criteria, bool loadComposition = false, bool filterConjunction = false)
		{
			var filter = (T)EntityReflector.GetFilterByFilterableColumns(typeof(T), typeof(T).GetProperties(), criteria);
			return new QueryBuilder<T>(this, filter, loadComposition, filterConjunction);
		}

		public IQuerySyncBuilder<T> SearchSync(object criteria, bool loadComposition = false, bool filterConjunction = false)
		{
			var filter = (T)EntityReflector.GetFilterByFilterableColumns(typeof(T), typeof(T).GetProperties(), criteria);
			return new QuerySyncBuilder<T>(this, filter, loadComposition, filterConjunction);
		}

		public IQueryPaginatedBuilder<T> Search(object criteria, int page, int pageSize, bool loadComposition = false, bool filterConjunction = false)
		{
			var filter = (T)EntityReflector.GetFilterByFilterableColumns(typeof(T), typeof(T).GetProperties(), criteria);
			return new QueryPaginatedBuilder<T>(this, filter, loadComposition, filterConjunction, page: page, pageSize: pageSize);
		}

		public IQueryPaginatedBuilder<T> SearchSync(object criteria, int page, int pageSize, bool loadComposition = false, bool filterConjunction = false)
		{
			var filter = (T)EntityReflector.GetFilterByFilterableColumns(typeof(T), typeof(T).GetProperties(), criteria);
			return new QueryPaginatedBuilder<T>(this, filter, loadComposition, filterConjunction, page: page, pageSize: pageSize);
		}

		public ICollection<T> BulkSearch(object[] criterias, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false)
		{
			var taskList = new List<Task>();
			ConcurrentDictionary<string, int> preResult = new ConcurrentDictionary<string, int>();

			if (criterias != null)
			{
				foreach (var criteria in criterias)
				{
					keepConnection = true;

					var newTask = Task.Run(async () =>
					{
						var queryResult = await Search(criteria, loadComposition);
						if (queryResult != null)
							foreach (var item in queryResult)
							{
								var jsonItem = JsonSerializer.Serialize(item);
								if (!preResult.ContainsKey(jsonItem))
									preResult.TryAdd(jsonItem, 1);
								else
									preResult[jsonItem] += 1;
							}
					});

					taskList.Add(newTask);
				}

				Task.WaitAll(taskList.ToArray());
				connection.Close();
			}

			var typedResult = preResult.OrderByDescending(rs => rs.Value)
									 .Select(rs => JsonSerializer.Deserialize<T>(rs.Key));

			var result = (recordsLimit > 0)
					   ? typedResult.Take(recordsLimit).ToList()
					   : typedResult.ToList();

			return result;
		}

		public ICollection<T> BulkSearchSync(object[] criterias, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false)
		{
			ConcurrentDictionary<string, int> preResult = new ConcurrentDictionary<string, int>();

			if (criterias != null)
			{
				foreach (var criteria in criterias)
				{
				var queryResult = SearchSync(criteria, loadComposition)
									.OrderBy(sortAttributes != null ? new[] { sortAttributes } : Array.Empty<string>())
									.ToList();
					if (queryResult != null)
						queryResult.AsParallel().ForAll(item =>
						{
							var jsonItem = JsonSerializer.Serialize(item);
							if (!preResult.ContainsKey(jsonItem))
								preResult.TryAdd(jsonItem, 1);
							else
								preResult[jsonItem] += 1;
						});
				}
			}

			var typedResult = preResult.OrderByDescending(rs => rs.Value)
							  		   .Select(rs => JsonSerializer.Deserialize<T>(rs.Key));

			var result = (recordsLimit > 0)
					   ? typedResult.Take(recordsLimit).ToList()
					   : typedResult.ToList();

			return result;
		}

		public async Task<PaginatedResult<T>> Search(object criteria, int page, int pageSize, bool loadComposition = false, string sortAttributes = null, bool orderDescending = false)
		{
			page = Math.Max(1, page);
			pageSize = Math.Max(1, pageSize);

			var filter = EntityReflector.GetFilterByFilterableColumns(typeof(T), typeof(T).GetProperties(), criteria);
			var totalCount = await CountObject(filter as object);

			int offset = (page - 1) * pageSize;
			var queryResult = await QueryObjectsPaged(filter, PersistenceAction.Query, loadComposition, totalCount, offset, pageSize, sortAttributes: sortAttributes, orderDescending: orderDescending);

			var items = new List<T>();
			if (queryResult != null)
				foreach (var item in queryResult)
					items.Add(item as T);

			return new PaginatedResult<T>(items, totalCount, page, pageSize);
		}

		public PaginatedResult<T> SearchSync(object criteria, int page, int pageSize, bool loadComposition = false, string sortAttributes = null, bool orderDescending = false)
			=> Search(criteria, page, pageSize, loadComposition, sortAttributes, orderDescending).GetAwaiter().GetResult();

		#endregion

		#region Query Builders — Query

		public IQueryBuilder<T> Query(T filter, bool loadComposition = false, bool filterConjunction = false)
			=> new QueryBuilder<T>(this, filter, loadComposition, filterConjunction);

		public IQueryPaginatedBuilder<T> Query(T filter, int page, int pageSize, bool loadComposition = false, bool filterConjunction = false)
			=> new QueryPaginatedBuilder<T>(this, filter, loadComposition, filterConjunction, page: page, pageSize: pageSize);

		public IQuerySyncBuilder<T> QuerySync(T filter, bool loadComposition = false, bool filterConjunction = false)
			=> new QuerySyncBuilder<T>(this, filter, loadComposition, filterConjunction);

		public IQueryPaginatedBuilder<T> QuerySync(T filter, int page, int pageSize, bool loadComposition = false, bool filterConjunction = false)
			=> new QueryPaginatedBuilder<T>(this, filter, loadComposition, filterConjunction, page: page, pageSize: pageSize);

		#endregion

		#region Query Builders — OrderBy / GroupBy

		public IQueryBuilder<T> OrderBy(params string[] sortAttributes)
			=> new QueryBuilder<T>(this, default(T)).OrderBy(sortAttributes);

		public IQueryBuilder<T> OrderByDescending(params string[] sortAttributes)
			=> new QueryBuilder<T>(this, default(T)).OrderByDescending(sortAttributes);

		public IQueryBuilder<T> GroupBy(string[] groupAttributes, Dictionary<string, DataAggregationType> aggregates = null)
			=> new QueryBuilder<T>(this, default(T)).GroupBy(groupAttributes, aggregates);

		#endregion

		#region Count / QueryRaw

		public async Task<int> Count(T filterEntity)
			=> await CountObject(filterEntity as object);

		public int CountSync(T filterEntity)
			=> CountObjectSync(filterEntity as object);

		public async Task<ICollection<T>> QueryRaw(string sql, Dictionary<string, object> parameters)
		{
			ValidateRawSql(sql, parameters);

			if (connection == null || connection.State != ConnectionState.Open)
				Connect();

			var result = new List<T>();
			var queryResult = await ExecuteQueryAsync(typeof(T), sql, parameters);
			if (queryResult != null)
				foreach (var item in queryResult)
					result.Add((T)item);

			if (!keepConnection) base.Disconnect();

			return result;
		}

		public async Task<PaginatedResult<T>> QueryRaw(string sql, string countSql, Dictionary<string, object> parameters, int page = 1, int pageSize = 20)
		{
			ValidateRawSql(sql, parameters);
			ValidateRawSql(countSql, parameters);

			if (connection == null || connection.State != ConnectionState.Open)
				Connect();

			var countResult = await ExecuteQueryAsync(typeof(T), countSql, parameters);
			var totalCount = countResult != null ? countResult.Count() : 0;

			var queryResult = await ExecuteQueryAsync(typeof(T), sql, parameters);

			var items = new List<T>();
			if (queryResult != null)
				foreach (var item in queryResult)
					items.Add((T)item);

			if (!keepConnection) base.Disconnect();

			return new PaginatedResult<T>(items, totalCount, page, pageSize);
		}

		public ICollection<T> QueryRawSync(string sql, Dictionary<string, object> parameters)
			=> QueryRaw(sql, parameters).GetAwaiter().GetResult();

		public PaginatedResult<T> QueryRawSync(string sql, string countSql, Dictionary<string, object> parameters, int page = 1, int pageSize = 20)
			=> QueryRaw(sql, countSql, parameters, page, pageSize).GetAwaiter().GetResult();

		private static void ValidateRawSql(string sql, Dictionary<string, object> parameters)
		{
			if (string.IsNullOrWhiteSpace(sql))
				throw new ArgumentException("SQL cannot be null or empty.");
			if (parameters == null)
				throw new ArgumentException("Parameters dictionary cannot be null. Use an empty dictionary if no parameters are needed.");

			var normalized = sql.TrimStart().ToUpperInvariant();

			if (!normalized.StartsWith("SELECT"))
				throw new ArgumentException("Only SELECT statements are allowed in QueryRaw.");
			if (sql.Contains(';'))
				throw new ArgumentException("Multiple statements are not allowed in QueryRaw.");
			if (sql.Contains("--") || sql.Contains("/*"))
				throw new ArgumentException("SQL comments are not allowed in QueryRaw.");

			var forbidden = new[] { "DROP ", "DELETE ", "TRUNCATE ", "ALTER ", "INSERT ", "UPDATE ", "CREATE ", "EXEC ", "EXECUTE " };
			foreach (var word in forbidden)
			{
				if (normalized.Contains(word))
					throw new ArgumentException($"Statement containing '{word.Trim()}' is not allowed in QueryRaw.");
			}
		}

		#endregion

		#region Internal — Builder helpers

		internal async Task<ICollection<T>> QueryWithBuilder(T filter, bool loadComposition, bool filterConjunction,
			string sortAttributes, bool orderDescending, string groupAttributes,
			Dictionary<string, DataAggregationType> aggregates = null)
		{
			var result = new List<T>();
			var queryResult = await QueryObjects(filter, PersistenceAction.Query, loadComposition,
				filterConjunction: filterConjunction, sortAttributes: sortAttributes,
				orderDescending: orderDescending, groupAttributes: groupAttributes, aggregates: aggregates);
			if (queryResult != null)
				foreach (var item in queryResult)
					result.Add(item as T);
			return result;
		}

		internal int CountWithBuilder(T filter, bool loadComposition, bool filterConjunction,
			string sortAttributes, bool orderDescending, string groupAttributes,
			Dictionary<string, DataAggregationType> aggregates = null)
			=> QueryCountObjects(filter, filterConjunction).GetAwaiter().GetResult();

		internal async Task<PaginatedResult<T>> QueryPaginatedWithBuilder(T filter, int page, int pageSize,
			bool loadComposition, bool filterConjunction, string sortAttributes, bool orderDescending,
			string groupAttributes = null, Dictionary<string, DataAggregationType> aggregates = null)
		{
			var totalCount = await CountObject(filter as object);
			int offset = (page - 1) * pageSize;
			var queryResult = await QueryObjectsPaged(filter, PersistenceAction.Query, loadComposition,
				totalCount, offset, pageSize, filterConjunction, sortAttributes: sortAttributes,
				orderDescending: orderDescending, groupAttributes: groupAttributes, aggregates: aggregates);

			var items = new List<T>();
			if (queryResult != null)
				foreach (var item in queryResult)
					items.Add(item as T);

			return new PaginatedResult<T>(items, totalCount, page, pageSize);
		}

		#endregion

		#region BWOQ

		public IBwoqQueryBuilder<T> QueryBwoq()
			=> new BwoqQueryBuilder<T>(this);

		#endregion
	}
}
