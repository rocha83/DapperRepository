using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using System.Reflection;
using System.IO;
using System.Data;
using System.Data.SQLite;
using Rochas.DapperRepository.Base;
using Rochas.DapperRepository.Helpers;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Specification.Interfaces;
using Rochas.DapperRepository.Specification.Annotations;
using System.Collections.Concurrent;

namespace Rochas.DapperRepository
{
	public class GenericRepository<T> : DataBaseConnection, IDisposable, IGenericRepository<T> where T : class
	{
		#region Declarations

		static readonly Type entityType = typeof(T);
		static readonly PropertyInfo[] entityProps = entityType.GetProperties();
		bool _readUncommited;
		bool _useCache;

		#endregion

		#region Constructors

		public GenericRepository(DatabaseEngine engine, string connectionString, string logPath = null, bool keepConnected = false, bool readUncommited = false, bool useCache = true, params string[] replicaConnStrings)
			: base(engine, connectionString, logPath, keepConnected, replicaConnStrings)
		{
			_readUncommited = readUncommited;
			_useCache = useCache;
		}

		public GenericRepository(IDbConnection dbConnection, string logPath = null, bool keepConnected = false, bool readUncommited = false, bool useCache = true, params string[] replicaConnStrings)
			: base(dbConnection, logPath, keepConnected, replicaConnStrings)
		{
			_readUncommited = readUncommited;
			_useCache = useCache;
		}

		#endregion

		#region Public Methods

		public void Initialize(string tableScript, string databaseFileName = null)
		{
			if (!string.IsNullOrWhiteSpace(databaseFileName))
			{
				if (File.Exists(databaseFileName))
					File.Delete(databaseFileName);

				SQLiteConnection.CreateFile(databaseFileName);
			}

			Connect();
			ExecuteCommand(tableScript);
			Disconnect();
		}

		public async Task<T> Get(object key, bool loadComposition = false)
		{
			if (EntityReflector.IsEmptyObjectValue(key))
				return null;

			var filter = EntityReflector.GetFilterByPrimaryKey(entityType, entityProps, key) as T;

			return await Get(filter, loadComposition);
		}

		public T GetSync(object key, bool loadComposition = false)
		{
			if (EntityReflector.IsEmptyObjectValue(key))
				return null;

			var filter = EntityReflector.GetFilterByPrimaryKey(entityType, entityProps, key) as T;

			return GetSync(filter, loadComposition);
		}

		public async Task<T> Get(T filter, bool loadComposition = false)
		{
			return await GetObject(filter, loadComposition) as T;
		}

		public T GetSync(T filter, bool loadComposition = false)
		{
			return GetObjectSync(filter, loadComposition) as T;
		}

		public async Task<ICollection<T>> Search(object criteria, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false)
		{
			var result = new List<T>();
			var filter = EntityReflector.GetFilterByFilterableColumns(entityType, entityProps, criteria);
			var queryResult = await QueryObjects(filter, PersistenceAction.Query, loadComposition, recordsLimit, sortAttributes: sortAttributes, orderDescending: orderDescending);
			if (queryResult != null)
				foreach (var item in queryResult)
					result.Add(item as T);

			return result;
		}

		public ICollection<T> SearchSync(object criteria, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false)
		{
			var result = new List<T>();
			var filter = EntityReflector.GetFilterByFilterableColumns(entityType, entityProps, criteria);
			var queryResult = QueryObjectsSync(filter, PersistenceAction.Query, loadComposition, recordsLimit, sortAttributes: sortAttributes, orderDescending: orderDescending);
			if (queryResult != null)
				foreach (var item in queryResult)
					result.Add(item as T);

			return result;
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
					var queryResult = SearchSync(criteria, loadComposition,
											 sortAttributes: sortAttributes,
											 orderDescending: orderDescending);
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

		public async Task<ICollection<T>> Query(T filter, bool loadComposition = false, int recordsLimit = 0, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false)
		{
			var result = new List<T>();
			var queryResult = await QueryObjects(filter, PersistenceAction.Query, loadComposition, recordsLimit, filterConjunction, sortAttributes: sortAttributes, orderDescending: orderDescending);
			if (queryResult != null)
				foreach (var item in queryResult)
					result.Add(item as T);

			return result;
		}

		public ICollection<T> QuerySync(T filter, bool loadComposition = false, int recordsLimit = 0, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false)
		{
			var result = new List<T>();
			var queryResult = QueryObjectsSync(filter, PersistenceAction.Query, loadComposition, recordsLimit, filterConjunction, sortAttributes: sortAttributes, orderDescending: orderDescending);
			if (queryResult != null)
				foreach (var item in queryResult)
					result.Add(item as T);

			return result;
		}

		public async Task<int> Add(T entity, bool persistComposition = false)
		{
			return await AddObject(entity, persistComposition);
		}
		public int AddSync(T entity, bool persistComposition = false)
		{
			return AddObjectSync(entity, persistComposition);
		}

		public async Task AddRange(IEnumerable<T> entities, bool persistComposition = false)
		{
			try
			{
				StartTransaction();

				foreach (var entity in entities)
					await AddObject(entity, persistComposition);

				CommitTransaction();
			}
			catch (Exception ex)
			{
				CancelTransaction();
				throw ex;
			}
		}
		public void AddRangeSync(IEnumerable<T> entities, bool persistComposition = false)
		{
			try
			{
				StartTransaction();

				foreach (var entity in entities)
					AddObjectSync(entity, persistComposition);

				CommitTransaction();
			}
			catch (Exception ex)
			{
				CancelTransaction();
				throw ex;
			}
		}

		public async Task BulkSqlCreateRange(ICollection<T> entities)
		{
			var entitiesTable = EntityReflector.GetDataTable<T>(entities);
			await ExecuteBulkCommandAsync(entitiesTable);
		}
		public void BulkSqlCreateRangeSync(ICollection<T> entities)
		{
			var entitiesTable = EntityReflector.GetDataTable<T>(entities);
			ExecuteBulkCommand(entitiesTable);
		}

		public async Task<int> Update(T entity, T filterEntity, bool persistComposition = false)
		{
			return await UpdateObject(entity, filterEntity, persistComposition);
		}

		public int UpdateSync(T entity, T filterEntity, bool persistComposition = false)
		{
			return UpdateObjectSync(entity, filterEntity, persistComposition);
		}

		public async Task<int> Remove(T filterEntity)
		{
			return await RemoveObjects(filterEntity as object);
		}
		public int RemoveSync(T filterEntity)
		{
			return RemoveObjectsSync(filterEntity as object);
		}

		public async Task<int> Count(T filterEntity)
		{
			return await CountObject(filterEntity as object);
		}

		public int CountSync(T filterEntity)
		{
			return CountObjectSync(filterEntity as object);
		}

		#endregion

		#region Helper Methods

		public new void Dispose()
		{
			base.Dispose();
			GC.ReRegisterForFinalize(this);
		}

		private async Task<object> GetObject(object filter, bool loadComposition = false)
		{
			if (filter == null)
				return null;

			var queryResult = await QueryObjects(filter, PersistenceAction.Get, loadComposition);
			return queryResult?.FirstOrDefault();
		}

		private object GetObjectSync(object filter, bool loadComposition = false)
		{
			if (filter == null)
				return null;

			return QueryObjectsSync(filter, PersistenceAction.Get, loadComposition)?.FirstOrDefault();
		}

		private async Task<IEnumerable<object>> QueryObjects(object filterEntity, PersistenceAction action, bool loadComposition = false, int recordLimit = 0, bool filterConjunction = false, bool onlyListableAttributes = false, string showAttributes = null, string groupAttributes = null, string sortAttributes = null, bool orderDescending = false)
		{
			IEnumerable<object> returnList = null;

			// Verifica a existência da(s) entidade(s) no cache antes de realizar a consulta em banco de dados
			if (IsCacheable(filterEntity))
				returnList = DataCache.Get(filterEntity) as IEnumerable<object>;

			if (returnList == null)
			{
				var sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, action, filterEntity, recordLimit, filterConjunction, onlyListableAttributes, showAttributes, groupAttributes, sortAttributes, orderDescending, _readUncommited);

				if (((connection != null) && keepConnection) || base.Connect())
				{
					await ExecuteQueryAsync(filterEntity.GetType(), sqlInstruction);

					// Getting database return using Dapper
					returnList = await ExecuteQueryAsync(filterEntity.GetType(), sqlInstruction);
				}

				if (!keepConnection) base.Disconnect();

				// Perform the composition data load when exists (Eager Loading)
				if (loadComposition && (returnList != null) && returnList.Any())
				{
					var itemProps = returnList.First().GetType().GetProperties();
					foreach (var item in returnList)
						FillComposition(item, itemProps);
				}

				// Caso seja uma entidade elegível, adiciona a mesma ao cache
				if ((returnList != null) && IsCacheable(filterEntity))
					DataCache.Put(filterEntity, returnList);
			}

			return returnList;
		}

		private IEnumerable<object> QueryObjectsSync(object filterEntity, PersistenceAction action, bool loadComposition = false, int recordLimit = 0, bool filterConjunction = false, bool onlyListableAttributes = false, string showAttributes = null, string groupAttributes = null, string sortAttributes = null, bool orderDescending = false)
		{
			return QueryObjects(filterEntity, action, loadComposition, recordLimit, filterConjunction, onlyListableAttributes, showAttributes, groupAttributes, sortAttributes, orderDescending).GetAwaiter().GetResult();
		}

		private async Task<int> AddObject(object entity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
		{
			string sqlInstruction;
			int lastInsertedId = 0;

			var entityType = entity.GetType();
			var entityProps = entityType.GetProperties();

			if (keepConnection || base.Connect(optionalConnConfig))
			{
				sqlInstruction = EntitySqlParser.ParseEntity(entity, engine, PersistenceAction.Add);

				if (persistComposition)
					base.StartTransaction();

				lastInsertedId = await ExecuteCommandAsync(sqlInstruction);

				if (persistComposition)
				{
					var entityKeyProp = EntityReflector.GetKeyColumn(entityProps);
					entityKeyProp.SetValue(entity, lastInsertedId);
					PersistComposition(entity, PersistenceAction.Add);
				}
				else
					if (!keepConnection) base.Disconnect();
			}

			CleanCacheableData(entity);

			// Async persistence of database replicas
			if (replicationEnabled && !isReplicating)
				AddReplicas(entity, entityProps, lastInsertedId, persistComposition);

			return lastInsertedId;
		}

		private int AddObjectSync(object entity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
		{
			return AddObject(entity, persistComposition, optionalConnConfig, isReplicating).GetAwaiter().GetResult();
		}

		private async Task<int> UpdateObject(object entity, object filterEntity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
		{
			int recordsAffected = 0;
			string sqlInstruction;

			var entityType = entity.GetType();
			var entityProps = entityType.GetProperties();

			if (keepConnection || base.Connect(optionalConnConfig))
			{
				sqlInstruction = EntitySqlParser.ParseEntity(entity, engine, PersistenceAction.Update, filterEntity);

				if (persistComposition)
					base.StartTransaction();

				recordsAffected = await ExecuteCommandAsync(sqlInstruction);

				if (persistComposition)
					PersistComposition(entity, PersistenceAction.Update, filterEntity);
				else
				if (!keepConnection) base.Disconnect();
			}

			CleanCacheableData(entity);

			// Async persistence of database replicas
			if (base.replicationEnabled && !isReplicating)
				UpdateReplicas(entity, filterEntity, entityProps, persistComposition);

			return recordsAffected;
		}

		private int UpdateObjectSync(object entity, object filterEntity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
		{
			return UpdateObject(entity, filterEntity, persistComposition, optionalConnConfig, isReplicating).GetAwaiter().GetResult();
		}

		private async Task<int> RemoveObjects(object filterEntity, string optionalConnConfig = "", bool isReplicating = false)
		{
			string sqlInstruction;
			int recordsAffected = 0;

			var entityType = filterEntity.GetType();
			var entityProps = entityType.GetProperties();

			if (keepConnection || base.Connect(optionalConnConfig))
			{
				sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, PersistenceAction.Remove, filterEntity);

				recordsAffected = await ExecuteCommandAsync(sqlInstruction);

				PersistComposition(filterEntity, PersistenceAction.Remove);

				if (!keepConnection) base.Disconnect();
			}

			CleanCacheableData(filterEntity);

			// Async exclusion of database replicas
			if (base.replicationEnabled && !isReplicating)
				RemoveReplicas(filterEntity, entityProps);

			return recordsAffected;
		}
		private int RemoveObjectsSync(object filterEntity, string optionalConnConfig = "", bool isReplicating = false)
		{
			return RemoveObjects(filterEntity, optionalConnConfig, isReplicating).GetAwaiter().GetResult();
		}

		public async Task<int> CountObject(object filterEntity)
		{
			int result = 0;

			// Getting SQL statement from Helper
			var sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, PersistenceAction.Count, filterEntity);

			if (keepConnection || Connect())
			{
				// Getting database return using Dapper
				result = await ExecuteCommandAsync(sqlInstruction);
			}

			if (!keepConnection) Disconnect();

			return result;
		}

		public int CountObjectSync(object filterEntity)
		{
			return CountObject(filterEntity).Result;
		}
		private void FillComposition(object loadedEntity, PropertyInfo[] entityProps)
		{
			var childEntities = EntityReflector.GetRelatedEntities(entityProps);

			foreach (var child in childEntities)
			{
				Type childEntityType = child.PropertyType;
				bool childEntityIsList = childEntityType.Name.Contains("IList");
				if (childEntityIsList)
					childEntityType = child.PropertyType.GetGenericArguments()[0];

				PropertyInfo[] childProps = childEntityType.GetProperties();

				object childEntityInstance = Activator.CreateInstance(childEntityType, true);

				RelatedEntityAttribute relationAttrib = EntityReflector.GetRelatedEntityAttribute(child);

				var keyColumn = EntityReflector.GetKeyColumn(entityProps);

				switch (relationAttrib.Cardinality)
				{
					case RelationCardinality.OneToOne:

						if (EntityReflector.SetChildForeignKeyValue(loadedEntity, entityProps, childEntityInstance,
																	childProps, relationAttrib.ForeignKeyAttribute))
							childEntityInstance = GetObjectSync(childEntityInstance, true);

						break;
					case RelationCardinality.ManyToOne:

						if (EntityReflector.SetParentForeignKeyValue(loadedEntity, entityProps, childEntityInstance,
																	 childProps, relationAttrib.ForeignKeyAttribute))
						{
							childEntityInstance = GetObjectSync(childEntityInstance, true);
						}

						break;
					case RelationCardinality.OneToMany:

						childProps = childEntityType.GetProperties();
						if (EntityReflector.SetChildForeignKeyValue(loadedEntity, entityProps, childEntityInstance,
																	childProps, relationAttrib.ForeignKeyAttribute))
						{
							childEntityInstance = QueryObjectsSync(childEntityInstance, PersistenceAction.Query, true);
						}

						break;
					case RelationCardinality.ManyToMany:

						var intermedyEntityInstance = Activator.CreateInstance(relationAttrib.IntermediaryEntity, true);
						var intermedyEntityProps = intermedyEntityInstance.GetType().GetProperties();

						if (intermedyEntityInstance != null)
						{
							if (EntityReflector.SetChildForeignKeyValue(loadedEntity, entityProps, intermedyEntityInstance,
																		intermedyEntityProps, relationAttrib.IntermediaryKeyAttribute))
							{
								var manyToManyRelations = QueryObjectsSync(intermedyEntityInstance, PersistenceAction.Get);
								var manyToManyResultList = EntityReflector.CreateTypedList(child);

								foreach (var relationInstance in manyToManyRelations)
								{
									if (EntityReflector.SetParentForeignKeyValue(relationInstance, intermedyEntityProps,
																				 childEntityInstance, childProps,
																				 relationAttrib.ForeignKeyAttribute))
									{
										var childRelationInstance = GetObjectSync(childEntityInstance);
										manyToManyResultList.Add(childRelationInstance);
									}
								}

								childEntityInstance = manyToManyResultList;
								childEntityIsList = false; // Typed list already created
							}
						}
						break;
				}

				SetParentChildEntity(loadedEntity, child, childEntityInstance, childEntityIsList);
			}
		}

		private List<string> ParseComposition(object entity, PersistenceAction action, object filterEntity)
		{
			List<string> result = new List<string>();
			var childEntities = EntityReflector.GetRelatedEntities(entityProps);

			foreach (PropertyInfo child in childEntities)
			{
				object childEntityInstance;
				object childEntityFilter = null;

				var relationAttrib = EntityReflector.GetRelatedEntityAttribute(child);

				childEntityInstance = child.GetValue(entity, null);
				var entityParent = entity; //(action != PersistenceAction.Update) ? entity : filterEntity

				if (childEntityInstance != null)
				{
					var childEntityType = childEntityInstance.GetType();
					if (!childEntityType.Name.Contains("List"))
					{
						var childProps = childEntityType.GetProperties();
						action = EntitySqlParser.SetPersistenceAction(childEntityInstance, EntityReflector.GetKeyColumn(childProps));

						if (action == PersistenceAction.Update)
						{
							childEntityFilter = Activator.CreateInstance(childEntityInstance.GetType());
							EntityReflector.SetFilterPrimaryKey(childEntityInstance, childProps, childEntityFilter);
						}

						if (relationAttrib.Cardinality == RelationCardinality.OneToOne)
							EntityReflector.SetChildForeignKeyValue(entityParent, entityProps, childEntityInstance,
															   childProps, relationAttrib.ForeignKeyAttribute);
						else // ManyToOne relation
							EntityReflector.SetChildForeignKeyValue(childEntityInstance, childProps, entityParent,
															   entityProps, relationAttrib.ForeignKeyAttribute);

						result.Add(EntitySqlParser.ParseEntity(childEntityInstance, engine, action));
					}
					else
					{
						var childListInstance = (IList)childEntityInstance;
						List<object> childFiltersList = new List<object>();

						if (childListInstance.Count > 0)
						{
							foreach (var listItem in childListInstance)
							{
								var listItemType = listItem.GetType();
								var listItemProps = listItemType.GetProperties();

								if (relationAttrib.Cardinality == RelationCardinality.OneToMany)
								{
									EntitySqlParser.ParseOneToManyRelation(childEntityFilter, listItem, listItemType,
																		   listItemProps, ref action, childFiltersList);

									EntityReflector.SetChildForeignKeyValue(entityParent, entityProps, listItem, listItemProps, relationAttrib.ForeignKeyAttribute);

									result.Add(EntitySqlParser.ParseEntity(listItem, engine, action));
								}
								else
								{
									var manyToEntity = EntitySqlParser.ParseManyToRelation(listItem, relationAttrib);

									EntityReflector.SetChildForeignKeyValue(entityParent, entityProps, manyToEntity, listItemProps, relationAttrib.ForeignKeyAttribute);

									var existRelation = this.GetObject(manyToEntity);

									if (existRelation != null) manyToEntity = existRelation;

									var manyToEntityProps = manyToEntity.GetType().GetProperties();
									action = EntitySqlParser.SetPersistenceAction(manyToEntity, EntityReflector.GetKeyColumn(manyToEntityProps));

									object existFilter = null;
									if (action == PersistenceAction.Update)
									{
										existFilter = Activator.CreateInstance(manyToEntity.GetType());
										EntityReflector.SetFilterPrimaryKey(manyToEntity, manyToEntityProps, existFilter);
										childFiltersList.Add(existFilter);
									}

									result.Add(EntitySqlParser.ParseEntity(manyToEntity, engine, action));
								}
							}
						}
						else
						{
							var childInstance = Activator.CreateInstance(childListInstance.GetType().GetGenericArguments()[0]);

							object childEntity;
							if (relationAttrib.Cardinality == RelationCardinality.ManyToMany)
								childEntity = EntitySqlParser.ParseManyToRelation(childInstance, relationAttrib);
							else
								childEntity = childInstance;

							var childProps = childEntity.GetType().GetProperties();

							EntityReflector.SetChildForeignKeyValue(entityParent, entityProps, childEntity, childProps, relationAttrib.ForeignKeyAttribute);

							childFiltersList.Add(childEntity);
						}
					}
				}
			}

			return result;
		}

		private void SetParentChildEntity(object loadedEntity, PropertyInfo child, object childEntityInstance, bool childEntityIsList)
		{
			if (childEntityInstance != null)
				if (!childEntityIsList)
					child.SetValue(loadedEntity, childEntityInstance, null);
				else
				{
					var childListInstance = (IList)childEntityInstance;
					if (childListInstance.Count > 0)
					{
						var childTypedList = EntityReflector.CreateTypedList(child);
						foreach (var listItem in childListInstance)
							childTypedList.Add(listItem);
						child.SetValue(loadedEntity, childTypedList, null);
					}
				}
		}

		private void AddReplicas(object entity, PropertyInfo[] entityProps, int lastInsertedId, bool persistComposition)
		{
			var entityColumnKey = EntityReflector.GetKeyColumn(entityProps);
			if (entityColumnKey != null)
				entityColumnKey.SetValue(entity, lastInsertedId, null);

			ParallelParam parallelParam = new ParallelParam()
			{
				Param1 = entity,
				Param2 = PersistenceAction.Add,
				Param3 = persistComposition
			};

			var replicationParallelDelegate = new ParameterizedThreadStart(PersistReplicasAsync);

			Parallelizer.StartNewProcess(replicationParallelDelegate, parallelParam);
		}

		private void UpdateReplicas(object entity, object filterEntity, PropertyInfo[] entityProps, bool persistComposition)
		{
			ParallelParam parallelParam = new ParallelParam()
			{
				Param1 = entity,
				Param2 = PersistenceAction.Update,
				Param3 = persistComposition,
				Param4 = filterEntity
			};

			var replicationParallelDelegate = new ParameterizedThreadStart(PersistReplicasAsync);

			Parallelizer.StartNewProcess(replicationParallelDelegate, parallelParam);

		}

		private void RemoveReplicas(object filterEntity, PropertyInfo[] entityProps)
		{
			ParallelParam parallelParam = new ParallelParam()
			{
				Param1 = filterEntity,
				Param2 = PersistenceAction.Remove
			};

			var replicationParallelDelegate = new ParameterizedThreadStart(PersistReplicasAsync);

			Parallelizer.StartNewProcess(replicationParallelDelegate, parallelParam);
		}

		private void PersistComposition(object entity, PersistenceAction action, object filterEntity = null)
		{
			try
			{
				List<string> childEntityCommands = ParseComposition(entity, action, filterEntity);

				foreach (var cmd in childEntityCommands)
					ExecuteCommand(cmd);

				if (base.transactionControl != null)
					base.CommitTransaction();

				if (!keepConnection) base.Disconnect();

				CleanCacheableData(entity);
			}
			catch (Exception)
			{
				if (base.transactionControl != null)
					base.CancelTransaction();
			}
		}

		private void PersistReplicasAsync(object param)
		{
			try
			{
				foreach (var connString in _replicaConnStrings)
				{
					ParallelParam parallelParam = param as ParallelParam;

					object entity = parallelParam.Param1;
					PersistenceAction action = (PersistenceAction)parallelParam.Param2;

					bool persistComposition = false;
					if (parallelParam.Param3 != null)
						persistComposition = (bool)parallelParam.Param3;

					object filterEntity = null;
					if (parallelParam.Param4 != null)
						filterEntity = parallelParam.Param4;

					using (var repos = new GenericRepository<T>(engine, _connString))
					{
						switch (action)
						{
							case PersistenceAction.Add:
								repos.AddObjectSync(entity, persistComposition, connString, true);
								break;
							case PersistenceAction.Update:
								repos.UpdateObjectSync(entity, filterEntity, persistComposition, connString, true);
								break;
							case PersistenceAction.Remove:
								repos.RemoveObjectsSync(entity, connString, true);
								break;
						}
					}
				}
			}
			catch (Exception ex)
			{
				RegisterException("PersistReplicas", ex, param);
			}
		}

		private bool IsCacheable(object entity)
		{
			var hasCacheAttrib = (entity.GetType().GetCustomAttribute(typeof(CacheableAttribute)) != null);
			return (hasCacheAttrib && _useCache);
		}

		private void CleanCacheableData(object entity)
		{
			var isCacheable = (entity.GetType().GetCustomAttribute(typeof(CacheableAttribute)) != null);
			if (isCacheable)
				DataCache.Del(entity, true);
		}

		private void RegisterException(string operationName, Exception exception, object content)
		{
			var logFileName = string.Format("{0}\\{1}_{2}_{3}.log", _logPath, operationName, content.GetHashCode(), DateTime.Now.Ticks);

			var exceptionContent = string.Format("Exception : {0}{1}{2} Content : {3}", JsonSerializer.Serialize(exception), Environment.NewLine, Environment.NewLine, JsonSerializer.Serialize(content));

			File.WriteAllText(logFileName, exceptionContent);
		}

		#endregion
	}
}
