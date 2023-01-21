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
using Rochas.DapperRepository.Helpers.SQL;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Specification.Interfaces;
using Rochas.DapperRepository.Specification.Annotations;

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
            var filter = EntityReflector.GetFilterByPrimaryKey(entityType, entityProps, key) as T;

            return await Get(filter, loadComposition);
        }

        public T GetSync(object key, bool loadComposition = false)
        {
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

        public async Task<ICollection<T>> Search(object criteria, bool loadComposition = false, int recordsLimit = 0, string orderAttributes = null, bool orderDescending = false)
        {
            var result = new List<T>();
            var filter = EntityReflector.GetFilterByFilterableColumns(entityType, entityProps, criteria);
            var queryResult = await ListObjects(filter, PersistenceAction.List, loadComposition, recordsLimit, orderAttributes: orderAttributes, orderDescending: orderDescending);
            if (queryResult != null)
                foreach (var item in queryResult)
                    result.Add(item as T);

            return result;
        }

        public ICollection<T> SearchSync(object criteria, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false)
        {
            var result = new List<T>();
            var filter = EntityReflector.GetFilterByFilterableColumns(entityType, entityProps, criteria);
            var queryResult = ListObjectsSync(filter, PersistenceAction.List, loadComposition, recordsLimit, sortAttributes: sortAttributes, orderDescending: orderDescending);
            if (queryResult != null)
                foreach (var item in queryResult)
                    result.Add(item as T);

            return result;
        }

        public async Task<ICollection<T>> List(T filter, bool loadComposition = false, int recordsLimit = 0, string orderAttributes = null, bool orderDescending = false)
        {
            var result = new List<T>();
            var queryResult = await ListObjects(filter, PersistenceAction.List, loadComposition, recordsLimit, orderAttributes: orderAttributes, orderDescending: orderDescending);
            if (queryResult != null)
                foreach (var item in queryResult)
                    result.Add(item as T);

            return result;
        }

        public ICollection<T> ListSync(T filter, bool loadComposition = false, int recordsLimit = 0, string sortAttributes = null, bool orderDescending = false)
        {
            var result = new List<T>();
            var queryResult = ListObjectsSync(filter, PersistenceAction.List, loadComposition, recordsLimit, sortAttributes: sortAttributes, orderDescending: orderDescending);
            if (queryResult != null)
                foreach (var item in queryResult)
                    result.Add(item as T);

            return result;
        }

        public async Task<int> Create(T entity, bool persistComposition = false)
        {
            return await CreateObject(entity, persistComposition);
        }
        public int CreateSync(T entity, bool persistComposition = false)
        {
            return CreateObjectSync(entity, persistComposition);
        }

        public async Task CreateRange(IEnumerable<T> entities, bool persistComposition = false)
        {
            try
            {
                StartTransaction();

                foreach (var entity in entities)
                    await CreateObject(entity, persistComposition);

                CommitTransaction();
            }
            catch (Exception ex)
            {
                CancelTransaction();
                throw ex;
            }
        }
        public void CreateRangeSync(IEnumerable<T> entities, bool persistComposition = false)
        {
            try
            {
                StartTransaction();

                foreach (var entity in entities)
                    CreateObjectSync(entity, persistComposition);

                CommitTransaction();
            }
            catch (Exception ex)
            {
                CancelTransaction();
                throw ex;
            }
        }

        public async Task CreateBulkRange(ICollection<T> entities)
        {
            var entitiesTable = EntityReflector.GetDataTable<T>(entities);
            await ExecuteBulkCommandAsync(entitiesTable);
        }
        public void CreateBulkRangeSync(ICollection<T> entities)
        {
            var entitiesTable = EntityReflector.GetDataTable<T>(entities);
            ExecuteBulkCommand(entitiesTable);
        }

        public async Task<int> Edit(T entity, T filterEntity, bool persistComposition = false)
        {
            return await EditObject(entity, filterEntity, persistComposition);
        }

        public int EditSync(T entity, T filterEntity, bool persistComposition = false)
        {
            return EditObjectSync(entity, filterEntity, persistComposition);
        }

        public async Task<int> Delete(T filterEntity)
        {
            return await Delete(filterEntity as object);
        }
        public int DeleteSync(T filterEntity)
        {
            return DeleteSync(filterEntity as object);
        }

        public async Task<int> Count(T filterEntity)
        {
            return await Count(filterEntity as object);
        }

        public int CountSync(T filterEntity)
        {
            return CountSync(filterEntity as object);
        }

        #endregion

        #region Helper Methods

        private async Task<object> GetObject(object filter, bool loadComposition = false)
        {
            var queryResult = await ListObjects(filter, PersistenceAction.Get, loadComposition);
            return queryResult?.FirstOrDefault();
        }

        private object GetObjectSync(object filter, bool loadComposition = false)
        {
            return ListObjectsSync(filter, PersistenceAction.Get, loadComposition)?.FirstOrDefault();
        }

        private async Task<IEnumerable<object>> ListObjects(object filterEntity, PersistenceAction action, bool loadComposition = false, int recordLimit = 0, bool onlyListableAttributes = false, string showAttributes = null, Dictionary<string, double[]> rangeValues = null, string groupAttributes = null, string orderAttributes = null, bool orderDescending = false)
        {
            IEnumerable<object> returnList = null;

            // Verifica a existência da(s) entidade(s) no cache antes de realizar a consulta em banco de dados
            if (IsCacheable(filterEntity))
                returnList = DataCache.Get(filterEntity) as IEnumerable<object>;

            if (returnList == null)
            {
                var sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, action, filterEntity, recordLimit, onlyListableAttributes, showAttributes, rangeValues, groupAttributes, orderAttributes, orderDescending, _readUncommited);

                if (keepConnection || base.Connect())
                {
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

        private IEnumerable<object> ListObjectsSync(object filterEntity, PersistenceAction action, bool loadComposition = false, int recordLimit = 0, bool onlyListableAttributes = false, string showAttributes = null, Dictionary<string, double[]> rangeValues = null, string groupAttributes = null, string sortAttributes = null, bool orderDescending = false)
        {
            IEnumerable<object> returnList = null;

            // Getting SQL statement from Helper
            var sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, action, filterEntity, recordLimit, onlyListableAttributes, showAttributes, rangeValues, groupAttributes, sortAttributes, orderDescending, _readUncommited);

            if (keepConnection || Connect())
            {
                // Getting database return using Dapper
                returnList = ExecuteQuery(filterEntity.GetType(), sqlInstruction);
            }

            if (!keepConnection) Disconnect();

            // Perform the composition data load when exists (Eager Loading)
            if (loadComposition && (returnList != null) && returnList.Any())
            {
                var itemProps = returnList.First().GetType().GetProperties();
                foreach (var item in returnList)
                    FillComposition(item, itemProps);
            }

            return returnList;
        }

        private async Task<int> CreateObject(object entity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
        {
            string sqlInstruction;
            int lastInsertedId = 0;

            var entityType = entity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(entity, engine, PersistenceAction.Create);

                if (persistComposition)
                    base.StartTransaction();

                lastInsertedId = await ExecuteCommandAsync(sqlInstruction);

                if (persistComposition)
                    PersistComposition(entity, PersistenceAction.Create);
                else
                    if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(entity);

            // Async persistence of database replicas
            if (replicationEnabled && !isReplicating)
                CreateReplicas(entity, entityProps, lastInsertedId, persistComposition);

            return lastInsertedId;
        }

        private int CreateObjectSync(object entity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
        {
            string sqlInstruction;
            int lastInsertedId = 0;

            var entityType = entity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(entity, engine, PersistenceAction.Create);

                if (persistComposition)
                    base.StartTransaction();

                lastInsertedId = ExecuteCommand(sqlInstruction);

                if (persistComposition)
                    PersistComposition(entity, PersistenceAction.Create);
                else
                    if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(entity);

            // Async persistence of database replicas
            if (replicationEnabled && !isReplicating)
                CreateReplicas(entity, entityProps, lastInsertedId, persistComposition);

            return lastInsertedId;
        }

        private async Task<int> EditObject(object entity, object filterEntity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
        {
            int recordsAffected = 0;
            string sqlInstruction;

            var entityType = entity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(entity, engine, PersistenceAction.Edit, filterEntity);

                if (persistComposition)
                    base.StartTransaction();

                recordsAffected = await ExecuteCommandAsync(sqlInstruction);

                if (persistComposition)
                    PersistComposition(entity, PersistenceAction.Edit);
                else
                if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(entity);

            // Async persistence of database replicas
            if (base.replicationEnabled && !isReplicating)
                EditReplicas(entity, filterEntity, entityProps, persistComposition);

            return recordsAffected;
        }

        private int EditObjectSync(object entity, object filterEntity, bool persistComposition, string optionalConnConfig = "", bool isReplicating = false)
        {
            int recordsAffected = 0;
            string sqlInstruction;

            var entityType = entity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(entity, engine, PersistenceAction.Edit, filterEntity);

                if (persistComposition)
                    base.StartTransaction();

                recordsAffected = ExecuteCommand(sqlInstruction);

                if (persistComposition)
                    PersistComposition(entity, PersistenceAction.Edit);
                else
                if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(entity);

            // Async persistence of database replicas
            if (base.replicationEnabled && !isReplicating)
                EditReplicas(entity, filterEntity, entityProps, persistComposition);

            return recordsAffected;
        }

        private async Task<int> Delete(object filterEntity, string optionalConnConfig = "", bool isReplicating = false)
        {
            string sqlInstruction;
            int recordsAffected = 0;

            var entityType = filterEntity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, PersistenceAction.Delete, filterEntity);

                recordsAffected = await ExecuteCommandAsync(sqlInstruction);

                PersistComposition(filterEntity, PersistenceAction.Delete);

                if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(filterEntity);

            // Async persistence of database replicas
            if (base.replicationEnabled && !isReplicating)
                DeleteReplicas(filterEntity, entityProps);

            return recordsAffected;
        }
        private int DeleteSync(object filterEntity, string optionalConnConfig = "", bool isReplicating = false)
        {
            string sqlInstruction;
            int recordsAffected = 0;

            var entityType = filterEntity.GetType();
            var entityProps = entityType.GetProperties();

            if (keepConnection || base.Connect(optionalConnConfig))
            {
                sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, PersistenceAction.Delete, filterEntity);

                recordsAffected = ExecuteCommand(sqlInstruction);

                PersistComposition(filterEntity, PersistenceAction.Delete);

                if (!keepConnection) base.Disconnect();
            }

            CleanCacheableData(filterEntity);

            // Async persistence of database replicas
            if (base.replicationEnabled && !isReplicating)
                DeleteReplicas(filterEntity, entityProps);

            return recordsAffected;
        }

        public async Task<int> Count(object filterEntity)
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

        public int CountSync(object filterEntity)
        {
            int result = 0;

            // Getting SQL statement from Helper
            var sqlInstruction = EntitySqlParser.ParseEntity(filterEntity, engine, PersistenceAction.Count, filterEntity);

            if (keepConnection || Connect())
            {
                // Getting database return using Dapper
                result = ExecuteCommand(sqlInstruction);
            }

            if (!keepConnection) Disconnect();

            return result;
        }
        private void FillComposition(object loadedEntity, PropertyInfo[] entityProps)
        {
            RelatedEntity relationConfig = null;

            var propertiesList = entityProps.Where(prp => prp.GetCustomAttributes(true)
                                            .Any(atb => atb.GetType().Name.Equals("RelatedEntity")));

            foreach (var prop in propertiesList)
            {
                object attributeInstance = null;

                IEnumerable<object> attributeAnnotations = prop.GetCustomAttributes(true)
                                                               .Where(atb => atb.GetType().Name.Equals("RelatedEntity"));

                foreach (object annotation in attributeAnnotations)
                {
                    relationConfig = (RelatedEntity)annotation;

                    PropertyInfo foreignKeyColumn = null;
                    object foreignKeyValue = null;

                    var keyColumn = EntityReflector.GetKeyColumn(entityProps);

                    switch (relationConfig.Cardinality)
                    {
                        case RelationCardinality.OneToOne:

                            attributeInstance = Activator.CreateInstance(prop.PropertyType);

                            foreignKeyColumn = loadedEntity.GetType().GetProperty(relationConfig.ForeignKeyAttribute);

                            foreignKeyValue = foreignKeyColumn.GetValue(loadedEntity, null);

                            if ((foreignKeyValue != null) && int.Parse(foreignKeyValue.ToString()) > 0)
                            {
                                var attributeProps = attributeInstance.GetType().GetProperties();
                                var keyColumnAttribute = EntityReflector.GetKeyColumn(attributeProps);

                                keyColumnAttribute.SetValue(attributeInstance, foreignKeyColumn.GetValue(loadedEntity, null), null);

                                attributeInstance = GetObject(attributeInstance);
                            }

                            break;
                        case RelationCardinality.OneToMany:

                            attributeInstance = Activator.CreateInstance(prop.PropertyType.GetGenericArguments()[0], true);

                            foreignKeyColumn = attributeInstance.GetType().GetProperty(relationConfig.ForeignKeyAttribute);
                            foreignKeyColumn.SetValue(attributeInstance, int.Parse(keyColumn.GetValue(loadedEntity, null).ToString()), null);

                            attributeInstance = ListObjects(attributeInstance as object, PersistenceAction.List);

                            break;
                        case RelationCardinality.ManyToMany:

                            attributeInstance = Activator.CreateInstance(relationConfig.IntermediaryEntity, true);

                            if (attributeInstance != null)
                            {
                                SetEntityForeignKey(loadedEntity, attributeInstance);

                                var manyToRelations = ListObjectsSync(attributeInstance, PersistenceAction.List, true);

                                Type childManyType = prop.PropertyType.GetGenericArguments()[0];
                                Type dynamicManyType = typeof(List<>).MakeGenericType(new Type[] { childManyType });
                                IList childManyEntities = (IList)Activator.CreateInstance(dynamicManyType, true);

                                foreach (var rel in manyToRelations)
                                {
                                    var childManyKeyValue = rel.GetType().GetProperty(relationConfig.IntermediaryKeyAttribute).GetValue(rel, null);
                                    var childFilter = Activator.CreateInstance(childManyType);

                                    var childFilterProps = childFilter.GetType().GetProperties();
                                    EntityReflector.GetKeyColumn(childFilterProps).SetValue(childFilter, childManyKeyValue, null);

                                    var childInstance = GetObject(childFilter);

                                    childManyEntities.Add(childInstance);
                                }

                                attributeInstance = childManyEntities;
                            }
                            break;
                    }
                }

                if (attributeInstance != null)
                    if (!prop.PropertyType.Name.Contains("List"))
                        prop.SetValue(loadedEntity, attributeInstance, null);
                    else
                        prop.SetValue(loadedEntity, (IList)attributeInstance, null);
            }
        }

        private List<string> ParseComposition(object entity, PersistenceAction action, object filterEntity)
        {
            List<string> result = new List<string>();
            object childEntityInstance = null;

            var entityType = entity.GetType();
            IEnumerable<PropertyInfo> childEntities = entityType.GetProperties().Where(prp => prp.GetCustomAttributes(true)
                                                                                .Any(atb => atb.GetType().Name.Equals("RelatedEntity")));

            foreach (PropertyInfo child in childEntities)
            {
                var relationAttrib = child.GetCustomAttributes(true)
                                          .FirstOrDefault(atb => atb.GetType().Name.Equals("RelatedEntity")) as RelatedEntity;

                childEntityInstance = child.GetValue(entity, null);
                object childEntityFilter = null;

                var entityParent = (action != PersistenceAction.Edit) ? entity : filterEntity;

                if (childEntityInstance != null)
                {
                    var childEntityType = childEntityInstance.GetType();

                    if (!childEntityType.Name.Contains("List"))
                    {
                        var childProps = childEntityType.GetProperties();
                        action = EntitySqlParser.SetPersistenceAction(childEntityInstance, EntityReflector.GetKeyColumn(childProps));
                        childEntityFilter = Activator.CreateInstance(childEntityInstance.GetType());

                        if (action == PersistenceAction.Edit)
                            EntityReflector.MigrateEntityPrimaryKey(childEntityInstance, childProps, childEntityFilter);

                        SetEntityForeignKey(entityParent, child);

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
                                if (relationAttrib.Cardinality == RelationCardinality.OneToMany)
                                {
                                    var listItemType = listItem.GetType();
                                    childEntityFilter = Activator.CreateInstance(listItemType);

                                    var listItemProps = listItemType.GetProperties();
                                    action = EntitySqlParser.SetPersistenceAction(listItem, EntityReflector.GetKeyColumn(listItemProps));

                                    if (action == PersistenceAction.Edit)
                                    {
                                        EntityReflector.MigrateEntityPrimaryKey(listItem, listItemProps, childEntityFilter);
                                        childFiltersList.Add(childEntityFilter);
                                    }

                                    SetEntityForeignKey(entityParent, listItem);

                                    result.Add(EntitySqlParser.ParseEntity(listItem, engine, action));
                                }
                                else
                                {
                                    var manyToEntity = EntitySqlParser.ParseManyToRelation(listItem, relationAttrib);

                                    SetEntityForeignKey(entityParent, manyToEntity);

                                    var existRelation = this.GetObject(manyToEntity);

                                    if (existRelation != null) manyToEntity = existRelation;

                                    var manyToEntityProps = manyToEntity.GetType().GetProperties();
                                    action = EntitySqlParser.SetPersistenceAction(manyToEntity, EntityReflector.GetKeyColumn(manyToEntityProps));

                                    object existFilter = null;
                                    if (action == PersistenceAction.Edit)
                                    {
                                        existFilter = Activator.CreateInstance(manyToEntity.GetType());
                                        EntityReflector.MigrateEntityPrimaryKey(manyToEntity, manyToEntityProps, existFilter);
                                        childFiltersList.Add(existFilter);
                                    }

                                    result.Add(EntitySqlParser.ParseEntity(manyToEntity, engine, action));
                                }
                            }
                        }
                        else
                        {
                            var childInstance = Activator.CreateInstance(childListInstance.GetType().GetGenericArguments()[0]);

                            var childEntity = new object();
                            if (relationAttrib.Cardinality == RelationCardinality.ManyToMany)
                                childEntity = EntitySqlParser.ParseManyToRelation(childInstance, relationAttrib);
                            else
                                childEntity = childInstance;

                            SetEntityForeignKey(entityParent, childEntity);

                            childFiltersList.Add(childEntity);
                        }
                    }
                }
            }

            if (result.Any(rst => rst.Contains(SQLStatements.SQL_ReservedWord_INSERT)))
                result.Reverse();

            return result;
        }

        private void CreateReplicas(object entity, PropertyInfo[] entityProps, int lastInsertedId, bool persistComposition)
        {
            var entityColumnKey = EntityReflector.GetKeyColumn(entityProps);
            if (entityColumnKey != null)
                entityColumnKey.SetValue(entity, lastInsertedId, null);

            ParallelParam parallelParam = new ParallelParam()
            {
                Param1 = entity,
                Param2 = PersistenceAction.Create,
                Param3 = persistComposition
            };

            var replicationParallelDelegate = new ParameterizedThreadStart(PersistReplicasAsync);

            Parallelizer.StartNewProcess(replicationParallelDelegate, parallelParam);
        }

        private void EditReplicas(object entity, object filterEntity, PropertyInfo[] entityProps, bool persistComposition)
        {
            ParallelParam parallelParam = new ParallelParam()
            {
                Param1 = entity,
                Param2 = PersistenceAction.Edit,
                Param3 = persistComposition,
                Param4 = filterEntity
            };

            var replicationParallelDelegate = new ParameterizedThreadStart(PersistReplicasAsync);

            Parallelizer.StartNewProcess(replicationParallelDelegate, parallelParam);

        }

        private void DeleteReplicas(object filterEntity, PropertyInfo[] entityProps)
        {
            ParallelParam parallelParam = new ParallelParam()
            {
                Param1 = filterEntity,
                Param2 = PersistenceAction.Delete
            };

            var replicationParallelDelegate = new ParameterizedThreadStart(PersistReplicasAsync);

            Parallelizer.StartNewProcess(replicationParallelDelegate, parallelParam);
        }

        private void SetEntityForeignKey(object parentEntity, object childEntity)
        {
            var parentProps = parentEntity.GetType().GetProperties();
            var parentKey = EntityReflector.GetKeyColumn(parentProps);

            var childProps = childEntity.GetType().GetProperties();
            var childForeignKey = EntityReflector.GetForeignKeyColumn(childProps);

            if ((parentKey != null) && (childForeignKey != null))
                childForeignKey.SetValue(childEntity, parentKey.GetValue(parentEntity, null), null);
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

                    using (var repos = new GenericRepository<T>(DatabaseEngine.SQLServer, _connString))
                    {
                        switch (action)
                        {
                            case PersistenceAction.Create:
                                repos.CreateObjectSync(entity, persistComposition, connString, true);
                                break;
                            case PersistenceAction.Edit:
                                repos.EditObjectSync(entity, filterEntity, persistComposition, connString, true);
                                break;
                            case PersistenceAction.Delete:
                                repos.DeleteSync(entity, connString, true);
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
