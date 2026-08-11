using System;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Rochas.DapperRepository.Specification.Annotations;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Exceptions;
using Rochas.DapperRepository.Helpers.SQL;
using static System.Collections.Specialized.BitVector32;

namespace Rochas.DapperRepository.Helpers
{
    public static class EntitySqlParser
    {
		#region Public Methods

		/// <summary>
		/// Parse entity model object instance to SQL ANSI CRUD statements
		/// </summary>
		/// <param name="entity">Entity model class reference</param>
		/// <param name="persistenceAction">Persistence action enum (Get, List, Create, Edit, Delete)</param>
		/// <param name="filterEntity">Filter entity model class reference</param>
		/// <param name="recordLimit">Result records limit</param>
		/// <param name="filterConjunction">Flag to filter entities attributes inclusively (AND operation)</param>
		/// <param name="onlyListableAttributes">Flag to return only attributes marked as listable</param>
		/// <param name="showAttributes">Comma separeted list of custom object attributes to show</param>
		/// <param name="groupAttributes">List of object attributes to group results</param>
		/// <param name="orderAttributes">List of object attributes to sort results</param>
		/// <param name="orderDescending">Flag to return ordering with descending order</param>
		/// <param name="readUncommited">Flag to set uncommited transaction level queries</param>
		/// <returns></returns>
		public static string ParseEntity(object entity, DatabaseEngine engine, PersistenceAction persistenceAction, object filterEntity = null, int recordLimit = 0, bool filterConjunction = false, bool onlyListableAttributes = false, string showAttributes = null, string groupAttributes = null, string sortAttributes = null, bool orderDescending = false, bool readUncommited = false, Dictionary<string, object> sqlParameters = null)
        {
            try
            {
                string sqlInstruction;
                string[] displayAttributes = new string[0];
                Dictionary<object, object> attributeColumnRelation;

                var entityType = entity.GetType();
                var entityProps = EntityReflector.GetEntityProperties(entityType);

                // Model validation
                if (!EntityReflector.VerifyTableAnnotation(entityType))
                    throw new InvalidOperationException("Entity table annotation not found. Please review model definition.");

                if (EntityReflector.GetKeyColumn(entityProps) == null)
                    throw new KeyNotFoundException("Entity key column annotation not found. Please review model definition.");
                //

                if (onlyListableAttributes)
                    EntityReflector.ValidateListableAttributes(entityProps, showAttributes, out displayAttributes);

                sqlInstruction = GetSqlInstruction(entity, entityType, entityProps, engine, persistenceAction, filterEntity,
                                                   recordLimit, filterConjunction, displayAttributes, groupAttributes, readUncommited, sqlParameters);

                if ((persistenceAction != PersistenceAction.Add) && (persistenceAction != PersistenceAction.Update))
				{
                    sqlInstruction = string.Format(sqlInstruction, ((engine != DatabaseEngine.SQLServer) && (recordLimit > 0))
                                   ? string.Format(SQLStatements.SQL_Action_LimitResult_MySQL, recordLimit)
                                   : string.Empty, "{0}", "{1}");

                    attributeColumnRelation = EntityReflector.GetPropertiesValueList(entity, entityType, entityProps, persistenceAction, engine);

                    if (!string.IsNullOrEmpty(groupAttributes))
                        ParseGroupingAttributes(attributeColumnRelation, groupAttributes, engine, ref sqlInstruction);
                    else
                        sqlInstruction = string.Format(sqlInstruction, string.Empty, "{0}");

                    if (!string.IsNullOrEmpty(sortAttributes))
                    {
                        var columnMapping = EntityReflector.GetColumnMapping(entityType, engine);
                        ParseOrdinationAttributes(columnMapping, sortAttributes, orderDescending, engine, ref sqlInstruction);
                    }
                    else
                        sqlInstruction = string.Format(sqlInstruction, string.Empty);
                }

                return sqlInstruction;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

		/// <summary>
		/// Parse entity model object instance to SQL ANSI CRUD statements with OFFSET/FETCH pagination
		/// </summary>
		public static string ParseEntityPaged(object entity, DatabaseEngine engine, PersistenceAction persistenceAction, object filterEntity = null, int offset = 0, int pageSize = 20, bool filterConjunction = false, string sortAttributes = null, bool orderDescending = false, bool readUncommited = false, Dictionary<string, object> sqlParameters = null)
        {
            try
            {
                string sqlInstruction;
                string[] displayAttributes = new string[0];
                Dictionary<object, object> attributeColumnRelation;

                var entityType = entity.GetType();
                var entityProps = EntityReflector.GetEntityProperties(entityType);

                if (!EntityReflector.VerifyTableAnnotation(entityType))
                    throw new InvalidOperationException("Entity table annotation not found.");

                if (EntityReflector.GetKeyColumn(entityProps) == null)
                    throw new KeyNotFoundException("Entity key column annotation not found.");

                sqlInstruction = GetSqlInstruction(entity, entityType, entityProps, engine, PersistenceAction.Query, filterEntity,
                                                   0, filterConjunction, displayAttributes, null, readUncommited, sqlParameters);

                if ((persistenceAction != PersistenceAction.Add) && (persistenceAction != PersistenceAction.Update))
				{
                    sqlInstruction = string.Format(sqlInstruction, string.Empty, "{0}", "{1}");

                    attributeColumnRelation = EntityReflector.GetPropertiesValueList(entity, entityType, entityProps, persistenceAction, engine);

                    sqlInstruction = string.Format(sqlInstruction, string.Empty, "{0}");

                    if (!string.IsNullOrEmpty(sortAttributes))
                    {
                        var columnMapping = EntityReflector.GetColumnMapping(entityType, engine);
                        ParseOrdinationAttributes(columnMapping, sortAttributes, orderDescending, engine, ref sqlInstruction);
                    }
                    else
                        sqlInstruction = string.Format(sqlInstruction, string.Empty);

                    // Adiciona OFFSET/FETCH para paginação
                    sqlInstruction = sqlInstruction.TrimEnd(';') + GetPaginationClause(engine, offset, pageSize);
                }

                return sqlInstruction;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

		private static string GetPaginationClause(DatabaseEngine engine, int offset, int pageSize)
		{
			switch (engine)
			{
				case DatabaseEngine.MySQL:
				case DatabaseEngine.SQLite:
					return string.Format(" LIMIT {0} OFFSET {1};", pageSize, offset);

				case DatabaseEngine.PostgreSQL:
					return string.Format(" LIMIT {0} OFFSET {1};", pageSize, offset);

				case DatabaseEngine.SQLServer:
					return string.Format(" OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY;", offset, pageSize);

				default:
					return string.Format(" LIMIT {0} OFFSET {1};", pageSize, offset);
			}
		}

        #endregion

        #region Helper Methods

        private static string GetSqlInstruction(object entity, Type entityType, PropertyInfo[] entityProps, DatabaseEngine engine, PersistenceAction action, object filterEntity, int recordLimit, bool filterConjunction, string[] showAttributes, string groupAttributes, bool readUncommited = false, Dictionary<string, object> sqlParameters = null)
        {
            string sqlInstruction;
            Dictionary<object, object> sqlFilterData;
            Dictionary<string, object[]> rangeValues = null;
            Dictionary<object, object> sqlEntityData = EntityReflector.GetPropertiesValueList(entity, entityType, entityProps, action, engine);

            if (filterEntity != null)
            {
                sqlFilterData = EntityReflector.GetPropertiesValueList(filterEntity, entityType, entityProps, action, engine);
                rangeValues = EntityReflector.GetEntityRangeFilter(filterEntity, entityProps);
            }
            else
                sqlFilterData = null;

            var keyColumnName = EntityReflector.GetKeyColumnName(entityProps);

            Dictionary<string, string> sqlResult = GetSqlParameters(sqlEntityData, engine, action, sqlFilterData,
                                                                        recordLimit, filterConjunction, showAttributes,
                                                                        keyColumnName, rangeValues, groupAttributes, readUncommited, sqlParameters);
            switch (action)
            {
                case PersistenceAction.Add:

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Create,
                                                   QuoteIdentifier(sqlResult["TableName"], engine),
                                                   sqlResult["ColumnList"],
                                                   sqlResult["ValueList"]);

                    break;

                case PersistenceAction.Update:

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Edit,
                                                   QuoteIdentifier(sqlResult["TableName"], engine),
                                                   sqlResult["ColumnValueList"],
                                                   sqlResult["ColumnFilterList"]);

                    break;

                case PersistenceAction.Remove:

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Delete,
                                                   QuoteIdentifier(sqlResult["TableName"], engine),
                                                   sqlResult["ColumnFilterList"]);

                    break;
                default: // Listagem, Consulta ou Count

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Query,
                                                   sqlResult["ColumnList"],
                                                   QuoteIdentifier(sqlResult["TableName"], engine),
                                                   sqlResult["RelationList"],
                                                   sqlResult["ColumnFilterList"],
                                                   "{0}", "{1}", "{2}");

                    break;
            }

            return sqlInstruction;
        }

        private static string QuoteIdentifier(string name, DatabaseEngine engine)
        {
            if (engine != DatabaseEngine.PostgreSQL) return name;
            if (string.IsNullOrWhiteSpace(name)) return name;
            return string.Join(".", name.Split('.').Select(p => $"\"{p}\""));
        }

        private static string BooleanLiteral(object value, DatabaseEngine engine)
        {
            if (value is bool boolVal)
                return (engine == DatabaseEngine.PostgreSQL) ? (boolVal ? "TRUE" : "FALSE") : (boolVal ? "1" : "0");
            return value.ToString();
        }

        private static void ParseGroupingAttributes(Dictionary<object, object> attributeColumnRelation, string groupAttributes, DatabaseEngine engine, ref string sqlInstruction)
        {
            string columnList = string.Empty;
            string complementaryColumnList = string.Empty;
            string[] groupingAttributes = groupAttributes.Split(',');

            for (int cont = 0; cont < groupingAttributes.Length; cont++)
                groupingAttributes[cont] = groupingAttributes[cont].Trim();

            foreach (var rel in attributeColumnRelation)
                if (Array.IndexOf(groupingAttributes, rel.Key) > -1)
                    columnList += string.Format("{0}, ", QuoteIdentifier(((KeyValuePair<object, object>)rel.Value).Key.ToString(), engine));
                else
                    if (!rel.Key.Equals("TableName"))
                    complementaryColumnList += string.Format("{0}, ", QuoteIdentifier(((KeyValuePair<object, object>)rel.Value).Key.ToString(), engine));

            if (!string.IsNullOrEmpty(columnList) && (columnList.Length > 2))
                columnList = columnList.Substring(0, columnList.Length - 2);
            if (!string.IsNullOrEmpty(complementaryColumnList) && (complementaryColumnList.Length > 2))
                complementaryColumnList = complementaryColumnList.Substring(0, complementaryColumnList.Length - 2);

            sqlInstruction = string.Format(sqlInstruction,
                                           string.Format(SQLStatements.SQL_Action_Group,
                                                         columnList, ", ", complementaryColumnList),
                                                         "{0}");
        }


        private static void ParseOrdinationAttributes(Dictionary<string, string> columnMapping, string sortAttributes, bool orderDescending, DatabaseEngine engine, ref string sqlInstruction)
        {
            string columnList = string.Empty;
            string[] ordinationAttributes = sortAttributes.Split(',');

            for (int contAtrib = 0; contAtrib < ordinationAttributes.Length; contAtrib++)
            {
                var propName = ordinationAttributes[contAtrib].Trim();

                if (columnMapping.TryGetValue(propName, out var columnName))
                    columnList = string.Concat(columnList, QuoteIdentifier(columnName, engine), ", ");
            }

            if (columnList.Length > 2)
            {
                columnList = columnList.Substring(0, columnList.Length - 2);

                sqlInstruction = string.Format(sqlInstruction,
                                               string.Format(SQLStatements.SQL_Action_OrderResult,
                                                             columnList,
                                                             orderDescending ? "DESC" : "ASC"));
            }
        }

        private static Dictionary<string, string> GetSqlParameters(Dictionary<object, object> entitySqlData, DatabaseEngine engine, PersistenceAction action, IDictionary<object, object> entitySqlFilter, int recordLimit, bool filterConjunction, string[] showAttributes, string keyColumnName, IDictionary<string, object[]> rangeValues, string groupAttributes, bool readUncommited = false, Dictionary<string, object> sqlParameters = null)
        {
            var returnDictionary = new Dictionary<string, string>();

            string tableName = string.Empty;
            string columnList = string.Empty;
            string valueList = string.Empty;
            string columnValueList = string.Empty;
            string columnFilterList = string.Empty;
            string relationList = string.Empty;

            string entityColumnName = string.Empty;
            string entityAttributeName = string.Empty;

            if (entitySqlData != null)
                foreach (var item in entitySqlData)
                {
                    var itemChildKeyPair = new KeyValuePair<object, object>();

                    // Grouping predicate
                    if (!item.Key.Equals("TableName"))
                    {
                        entityAttributeName = item.Key.ToString();
                        itemChildKeyPair = (KeyValuePair<object, object>)item.Value;
                        entityColumnName = ((KeyValuePair<object, object>)item.Value).Key.ToString();

                        if (!string.IsNullOrWhiteSpace(groupAttributes) && groupAttributes.Contains(entityAttributeName))
                            columnList += string.Format("{0}.{1}, ", QuoteIdentifier(tableName, engine), QuoteIdentifier(entityColumnName, engine));
                    }

                    if (item.Key.Equals("TableName"))
                    {
                        returnDictionary.Add(item.Key.ToString(), item.Value.ToString());
                        tableName = item.Value.ToString();
                    }
                    else if (itemChildKeyPair.Key is RelationalColumn)
                    {
                        SetRelationalSqlParameters(itemChildKeyPair, tableName, engine, ref columnList, ref relationList);
                    }
                    else if (itemChildKeyPair.Key is DataAggregationColumn)
                    {
                        SetAggregationSqlParameters(itemChildKeyPair, tableName, entityAttributeName, engine, ref columnList);
                    }
                    else
                    {
                        SetPredicateSqlParameters(itemChildKeyPair, engine, action, tableName, keyColumnName, entityColumnName, entityAttributeName,
                                                  recordLimit, showAttributes, ref columnList, ref valueList, ref columnValueList);
                    }
                }

            if (entitySqlFilter != null)
            {
                SetFilterSqlParameters(entitySqlFilter, tableName, action, rangeValues, ref columnFilterList, filterConjunction, sqlParameters, keyColumnName, engine);
            }

            FillSqlParametersResult(returnDictionary, action, ref columnList, ref valueList, ref columnValueList, ref columnFilterList, ref relationList, readUncommited);

            return returnDictionary;
        }

        public static void ParseOneToManyRelation(object childEntityFilter, object listItem, Type listItemType, PropertyInfo[] listItemProps,
                                                  ref PersistenceAction action, List<object> childFiltersList)
        {
            childEntityFilter = Activator.CreateInstance(listItemType);

            action = SetPersistenceAction(listItem, EntityReflector.GetKeyColumn(listItemProps));

            if (action == PersistenceAction.Update)
            {
                EntityReflector.SetFilterPrimaryKey(listItem, listItemProps, childEntityFilter);
                childFiltersList.Add(childEntityFilter);
            }
        }

        public static object ParseManyToRelation(object childEntity, RelatedEntityAttribute relation)
        {
            object result = null;
            var relEntity = relation.IntermediaryEntity;

            if (relEntity != null)
            {
                var interEntity = Activator.CreateInstance(relation.IntermediaryEntity);

                var childProps = childEntity.GetType().GetProperties();
                var childKey = EntityReflector.GetKeyColumn(childProps);
                var interKeyAttrib = interEntity.GetType().GetProperties()
                                                .FirstOrDefault(atb => atb.Name.Equals(relation.IntermediaryKeyAttribute));

                interKeyAttrib.SetValue(interEntity, childKey.GetValue(childEntity, null), null);

                result = interEntity;
            }

            return result;
        }

        public static PersistenceAction SetPersistenceAction(object entity, PropertyInfo entityKeyColumn)
        {
            return (entityKeyColumn.GetValue(entity, null).ToString().Equals(SqlDefaultValue.Zero))
                    ? PersistenceAction.Add : PersistenceAction.Update;
        }

        private static void FillSqlParametersResult(IDictionary<string, string> returnDictionary, PersistenceAction action, ref string columnList, ref string valueList, ref string columnValueList, ref string columnFilterList, ref string relationList, bool readUncommited = false)
        {
            if (action == PersistenceAction.Add)
            {
                columnList = columnList.Substring(0, columnList.Length - 2);
                valueList = valueList.Substring(0, valueList.Length - 2);

                returnDictionary.Add("ColumnList", columnList);
                returnDictionary.Add("ValueList", valueList);
            }
            else
            {
                if ((action == PersistenceAction.Query)
                    || (action == PersistenceAction.Get)
                    || (action == PersistenceAction.Count))
                {
                    columnList = columnList.Substring(0, columnList.Length - 2);
                    returnDictionary.Add("ColumnList", columnList);
                    returnDictionary.Add("RelationList", relationList);
                }
                else if (!string.IsNullOrEmpty(columnValueList))
                {
                    columnValueList = columnValueList.Substring(0, columnValueList.Length - 2);
                    returnDictionary.Add("ColumnValueList", columnValueList);
                }

                if (!string.IsNullOrEmpty(columnFilterList))
                {
                    returnDictionary.Add("ColumnFilterList", columnFilterList);
                }
                else
                    returnDictionary.Add("ColumnFilterList", "1 = 1");
            }
        }

        private static void SetPredicateSqlParameters(KeyValuePair<object, object> itemChildKeyPair, DatabaseEngine engine, PersistenceAction action, string tableName, string keyColumnName, string entityColumnName, string entityAttributeName, int recordLimit, string[] showAttributes, ref string columnList, ref string valueList, ref string columnValueList)
        {
            object entityColumnValue = itemChildKeyPair.Value;
            var isCustomColumn = !entityAttributeName.Equals(entityColumnName);

            if ((showAttributes != null) && (showAttributes.Length > 0))
                for (int counter = 0; counter < showAttributes.Length; counter++)
                    showAttributes[counter] = showAttributes[counter].Trim();

            switch (action)
            {
                case PersistenceAction.Add:
                    columnList += string.Format("{0}, ", QuoteIdentifier(entityColumnName, engine));
                    valueList += string.Format("{0}, ", BooleanLiteral(entityColumnValue, engine));

                    break;
                case PersistenceAction.Query:

                    if ((engine == DatabaseEngine.SQLServer) && string.IsNullOrWhiteSpace(columnList) && (recordLimit > 0))
                        columnList += string.Format(SQLStatements.SQL_Action_LimitResult, recordLimit);

                    if (((showAttributes == null) || (showAttributes.Length == 0))
                        || showAttributes.Length > 0 && Array.IndexOf(showAttributes, entityAttributeName) > -1)
                    {
                        var columnAlias = isCustomColumn ? string.Format(" AS {0}", entityAttributeName) : string.Empty;
                        columnList += string.Format("{0}.{1}{2}, ", QuoteIdentifier(tableName, engine), QuoteIdentifier(entityColumnName, engine), columnAlias);
                    }

                    break;
                case PersistenceAction.Get:

                    if (((showAttributes == null) || showAttributes.Length == 0)
                        || showAttributes.Length > 0 && Array.IndexOf(showAttributes, entityAttributeName) > -1)
                    {
                        var columnAlias = isCustomColumn ? string.Format(" AS {0}", entityAttributeName) : string.Empty;
                        columnList += string.Format("{0}.{1}{2}, ", QuoteIdentifier(tableName, engine), QuoteIdentifier(entityColumnName, engine), columnAlias);
                    }

                    break;
                case PersistenceAction.Count:

                    if (entityColumnName.Equals(keyColumnName))
                        columnList += string.Format(SQLStatements.SQL_Action_CountAggregation,
                                                    QuoteIdentifier(tableName, engine), QuoteIdentifier(entityColumnName, engine), entityAttributeName);

                    break;
                default: // Alteração e Exclusão
                    if (!entityAttributeName.ToLower().Equals("id"))
                    {
                        if (entityColumnValue == null)
                            entityColumnValue = SqlDefaultValue.Null;

                        columnValueList += string.Format("{0} = {1}, ", QuoteIdentifier(entityColumnName, engine), BooleanLiteral(entityColumnValue, engine));
                    }

                    break;
            }
        }

        private static void SetFilterSqlParameters(IDictionary<object, object> entitySqlFilter, string tableName, PersistenceAction action, IDictionary<string, object[]> rangeValues, ref string columnFilterList, bool filterConjunction, Dictionary<string, object> sqlParameters = null, string keyColumnName = null, DatabaseEngine engine = DatabaseEngine.SQLite)
        {
            int paramCounter = 0;
            foreach (var filter in entitySqlFilter)
            {
                if (!filter.Key.Equals("TableName") && !filter.Key.Equals("RelatedEntityAttribute"))
                {
                    object filterColumnName = null;
                    object filterColumnValue = null;
                    object columnName = null;
                    string columnNameStr = string.Empty;

                    var itemChildKeyPair = (KeyValuePair<object, object>)filter.Value;
                    if (itemChildKeyPair.Value is Array arrValue && !(itemChildKeyPair.Value is byte[]))
                    {
                        if (arrValue.Length == 1 && arrValue.GetValue(0) is string likePattern
                            && likePattern.StartsWith("%") && likePattern.EndsWith("%"))
                        {
                            string colName = itemChildKeyPair.Key is PropertyInfo pi
                                ? pi.Name
                                : itemChildKeyPair.Key is RelationalColumn rc ? rc.ColumnName
                                : itemChildKeyPair.Key?.ToString();

                            if (colName != null && (action == PersistenceAction.Query || action == PersistenceAction.Count))
                            {
                                var sqlCol = string.Format("{0}.{1}", QuoteIdentifier(tableName, engine), QuoteIdentifier(colName, engine));
                                var paramName = $"@p{paramCounter++}";
                                sqlParameters.Add(paramName, likePattern);
                                columnFilterList += string.Format("{0} LIKE {1}", sqlCol, paramName)
                                    + ((filterConjunction) ? SqlOperator.And : SqlOperator.Or);
                            }
                        }
                        continue;
                    }
                    if (itemChildKeyPair.Key is DataAggregationColumn)
                    {
                        continue;
                    }
                    else if (!(itemChildKeyPair.Key is RelationalColumn))
                    {
                        columnName = itemChildKeyPair.Key;
                        filterColumnName = string.Format("{0}.{1}", QuoteIdentifier(tableName, engine), QuoteIdentifier(columnName.ToString(), engine));
                        filterColumnValue = itemChildKeyPair.Value;
                    }
                    else
                    {
                        RelationalColumn relationConfig = itemChildKeyPair.Key as RelationalColumn;

                        if ((action == PersistenceAction.Query) && relationConfig.Filterable)
                        {
                            filterColumnName = string.Format("{0}.{1}", QuoteIdentifier(relationConfig.TableName, engine), QuoteIdentifier(relationConfig.ColumnName, engine));
                            filterColumnValue = itemChildKeyPair.Value;
                        }
                    }

                    var rangeFilter = false;
                    if (rangeValues != null && columnName != null)
                    {
                        columnNameStr = columnName.ToString();
                        rangeFilter = rangeValues.ContainsKey(columnNameStr);
                    }

                    if (((filterColumnValue != null)
                            && (filterColumnValue.ToString() != SqlDefaultValue.Null)
                            && (filterColumnValue.ToString() != SqlDefaultValue.Zero)
                            && (filterColumnValue.ToString() != "''")
                            && !(filterColumnValue is Guid guidVal && guidVal == Guid.Empty)
                            && !(filterColumnValue is DateTime dtVal && dtVal == DateTime.MinValue))
                        || rangeFilter)
                    {
                        var filterColumnNameLower = filterColumnName.ToString().ToLower();
                        var isKeyColumn = string.Equals(filterColumnName.ToString(), string.Concat(tableName, ".", keyColumnName), StringComparison.OrdinalIgnoreCase);
                        var isForeignKey = filterColumnNameLower.EndsWith("_id") || filterColumnNameLower.EndsWith(".id");

                        bool compareRule = ((action == PersistenceAction.Query)
                                             || (action == PersistenceAction.Count))
                                         && !(filterColumnValue is bool)
                                         && !(filterColumnValue is DateTime)
                                         && !(filterColumnValue is DateTimeOffset)
                                         && !(filterColumnValue is double)
                                         && !(filterColumnValue is float)
                                         && !(filterColumnValue is decimal)
                                         && !long.TryParse(filterColumnValue.ToString(), out long fake)
                                         && !filterColumnNameLower.Contains("date")
                                         && !isKeyColumn
                                         && !isForeignKey;

                        string comparation = string.Empty;

                        var filterColumnValueStr = filterColumnValue.ToString()
                                                                    .Replace("(", string.Empty)
                                                                    .Replace(")", string.Empty);

						if (!rangeFilter)
                        {
                            if (compareRule && sqlParameters != null)
                            {
                                // Parâmetro: valor com wildcards vai para o dictionary, SQL usa @pN
                                var paramName = $"@p{paramCounter++}";
                                var rawValue = filterColumnValueStr.Replace("'", string.Empty);
                                // Garante wildcards LIKE em todos os caminhos (Query, Search, Count)
                                if (!rawValue.StartsWith("%")) rawValue = "%" + rawValue;
                                if (!rawValue.EndsWith("%")) rawValue = rawValue + "%";
                                sqlParameters[paramName] = rawValue;
                                comparation = string.Format(SqlOperator.Contains, paramName);
                            }
                            else
                            {
                                comparation = (compareRule)
                                              ? string.Format(SqlOperator.Contains, filterColumnValueStr.Replace("'", string.Empty))
                                              : string.Concat(SqlOperator.Equal, filterColumnValueStr);
                            }

                             if (filterColumnValue is bool)
                                comparation = string.Concat(" = ", BooleanLiteral(filterColumnValue, engine));

                            if (!filterColumnValue.Equals(false))
                                columnFilterList += filterColumnName + comparation +
                                    ((compareRule && !filterConjunction) ? SqlOperator.Or : SqlOperator.And);
                        }
                        else
                            SetRangeFilterSql(filter, rangeValues, columnNameStr, 
                                              filterColumnName.ToString(), engine, ref columnFilterList);
                    }
                }
			}

            FinishFilterSql(ref columnFilterList, action);
		}

        private static void FinishFilterSql(ref string columnFilterList, PersistenceAction action)
        {
            if (!string.IsNullOrWhiteSpace(columnFilterList))
            {
				var tokenRemove = (columnFilterList.Trim().EndsWith(SqlOperator.And))
							       ? SqlOperator.And.Length : SqlOperator.Or.Length;

				columnFilterList = columnFilterList.Substring(0, columnFilterList.Length - tokenRemove);

				for (int count = 1; count <= columnFilterList.Count(fl => fl.Equals('(')); count++)
					columnFilterList += ")";
			}
		}

        private static void SetRangeFilterSql(KeyValuePair<object, object> filter,
                                            IDictionary<string, object[]> rangeValues, 
                                            string columnNameStr, string filterColumnName,
                                            DatabaseEngine engine,
                                            ref string columnFilterList)
        {
            string rangeFrom = "'{0}'";
            string rangeTo = "'{0}'";
            string comparation = string.Empty;

            var rangeFromValue = rangeValues[columnNameStr][0];
            var isNumericRange = rangeFromValue is not null && double.TryParse(rangeFromValue.ToString(), out var fake1);
            var isDateRange = !isNumericRange && rangeFromValue is DateTime;

            if (isNumericRange)
                comparation = GetNumericRangeComparation(rangeValues, columnNameStr, ref rangeFrom, ref rangeTo);
            else if (isDateRange)
                comparation = GetDateRangeComparation(rangeValues, columnNameStr, ref rangeFrom, ref rangeTo);
            
            if (!string.IsNullOrWhiteSpace(comparation))
                columnFilterList += string.Concat(QuoteIdentifier(columnNameStr, engine), " ", comparation, SqlOperator.And);
        }

        private static string GetNumericRangeComparation(IDictionary<string, object[]> rangeValues,
                                                         string columnNameStr, ref string rangeFrom, 
                                                         ref string rangeTo)
        {
            var result = string.Empty;
            rangeFrom = rangeFrom.Replace("'", string.Empty);
            rangeTo = rangeTo.Replace("'", string.Empty);

            var emptyRangeFrom = double.Parse(rangeValues[columnNameStr][0].ToString()) == 0;
            var emptyRangeTo = double.Parse(rangeValues[columnNameStr][1].ToString()) == 0;

            if (!emptyRangeFrom && !emptyRangeTo)
            {
                rangeFrom = string.Format(rangeFrom,
                rangeValues[columnNameStr][0].ToString());
                rangeTo = string.Format(rangeTo,
                rangeValues[columnNameStr][1].ToString());

                result = string.Format(SqlOperator.Between, rangeFrom, rangeTo);
            }
            else if (!emptyRangeFrom && emptyRangeTo)
            {
                rangeFrom = string.Format(rangeFrom,
                rangeValues[columnNameStr][0].ToString());

                result = string.Concat(SqlOperator.MajorOrEqual, rangeFrom);
            }
            else if (emptyRangeFrom && !emptyRangeTo)
            {
                rangeTo = string.Format(rangeTo,
                rangeValues[columnNameStr][1].ToString());

                result = string.Concat(SqlOperator.LessOrEqual, rangeTo);
            }

            return result;
        }

        private static string GetDateRangeComparation(IDictionary<string, object[]> rangeValues, 
                                                      string columnNameStr, ref string rangeFrom, 
                                                      ref string rangeTo)
        {
            var result = string.Empty;
            var fromVal = rangeValues[columnNameStr][0];
            var toVal = rangeValues[columnNameStr][1];
            var emptyRangeFrom = fromVal is null || (DateTime)fromVal == DateTime.MinValue;
            var emptyRangeTo = toVal is null || (DateTime)toVal == DateTime.MinValue;

            if (!emptyRangeFrom && !emptyRangeTo)
            {
                rangeFrom = string.Format(rangeFrom,
                ((DateTime)rangeValues[columnNameStr][0]).ToString(DateTimeFormat.NormalDate));
                rangeTo = string.Format(rangeTo,
                    ((DateTime)rangeValues[columnNameStr][1]).ToString(DateTimeFormat.NormalDate));

                result = string.Format(SqlOperator.Between, rangeFrom, rangeTo);
            }
            else if (!emptyRangeFrom && emptyRangeTo)
            {
                rangeFrom = string.Format(rangeFrom,
                ((DateTime)rangeValues[columnNameStr][0]).ToString(DateTimeFormat.NormalDate));

                result = string.Concat(SqlOperator.MajorOrEqual, rangeFrom);
            }
            else if (emptyRangeFrom && !emptyRangeTo)
            {
                rangeTo = string.Format(rangeTo,
                ((DateTime)rangeValues[columnNameStr][1]).ToString(DateTimeFormat.NormalDate));

                result = string.Concat(SqlOperator.LessOrEqual, rangeTo);
            }

            return result;
        }

        private static void SetRelationalSqlParameters(KeyValuePair<object, object> itemChildKeyPair, string tableName, DatabaseEngine engine, ref string columnList, ref string relationList)
        {
            string relation;
            RelationalColumn relationConfig = itemChildKeyPair.Key as RelationalColumn;

            columnList += string.Format("{0}.{1} ", QuoteIdentifier(relationConfig.TableName, engine), QuoteIdentifier(relationConfig.ColumnName, engine));

            if (!string.IsNullOrEmpty(relationConfig.ColumnAlias))
                columnList += string.Format(SQLStatements.SQL_Action_ColumnAlias, relationConfig.ColumnAlias);

            columnList += ", ";

            if (relationConfig.JunctionType == RelationalJunctionType.Mandatory)
            {
                relation = string.Format(SQLStatements.SQL_Action_RelationateMandatorily,
                                                       QuoteIdentifier(relationConfig.TableName, engine),
                                                       string.Format("{0}.{1}", QuoteIdentifier(tableName, engine), QuoteIdentifier(relationConfig.KeyColumn, engine)),
                                                       string.Format("{0}.{1}", QuoteIdentifier(relationConfig.TableName, engine),
                                                       QuoteIdentifier(relationConfig.ForeignKeyColumn, engine)));
            }
            else
            {
                if (!string.IsNullOrEmpty(relationConfig.IntermediaryColumnName))
                {
                    relation = string.Format(SQLStatements.SQL_Action_RelationateOptionally,
                                             QuoteIdentifier(relationConfig.IntermediaryColumnName, engine),
                                             string.Format("{0}.{1}", QuoteIdentifier(tableName, engine), QuoteIdentifier(relationConfig.ForeignKeyColumn, engine)),
                                             string.Format("{0}.{1}", QuoteIdentifier(relationConfig.IntermediaryColumnName, engine),
                                             QuoteIdentifier(relationConfig.ForeignKeyColumn, engine)));

                    relation += string.Format(SQLStatements.SQL_Action_RelationateOptionally,
                                              QuoteIdentifier(relationConfig.TableName, engine),
                                              string.Format("{0}.{1}", QuoteIdentifier(relationConfig.IntermediaryColumnName, engine), QuoteIdentifier(relationConfig.KeyColumn, engine)),
                                              string.Format("{0}.{1}", QuoteIdentifier(relationConfig.TableName, engine), QuoteIdentifier(relationConfig.ForeignKeyColumn, engine)));
                }
                else
                {
                    relation = string.Format(SQLStatements.SQL_Action_RelationateOptionally,
                                             QuoteIdentifier(relationConfig.TableName, engine),
                                             string.Format("{0}.{1}", QuoteIdentifier(tableName, engine), QuoteIdentifier(relationConfig.KeyColumn, engine)),
                                             string.Format("{0}.{1}", QuoteIdentifier(relationConfig.TableName, engine), QuoteIdentifier(relationConfig.ForeignKeyColumn, engine)));
                }
            }

            if (relation.Contains(relationList)
                || string.IsNullOrEmpty(relationList))
                relationList = relation;
            else if (!relationList.Contains(relation))
                relationList += " " + relation;
        }

        private static void SetAggregationSqlParameters(KeyValuePair<object, object> itemChildKeyPair, string tableName, string entityAttributeName, DatabaseEngine engine, ref string columnList)
        {
            var annotation = itemChildKeyPair.Key as DataAggregationColumn;

            switch (annotation.AggregationType)
            {
                case DataAggregationType.Count:
                    columnList += string.Format(SQLStatements.SQL_Action_CountAggregation,
                                                QuoteIdentifier(tableName, engine), QuoteIdentifier(annotation.ColumnName, engine), entityAttributeName);
                    break;
                case DataAggregationType.Sum:
                    columnList += string.Format(SQLStatements.SQL_Action_SummaryAggregation,
                                                QuoteIdentifier(tableName, engine), QuoteIdentifier(annotation.ColumnName, engine), entityAttributeName);
                    break;
                case DataAggregationType.Average:
                    columnList += string.Format(SQLStatements.SQL_Action_AverageAggregation,
                                                QuoteIdentifier(tableName, engine), QuoteIdentifier(annotation.ColumnName, engine), entityAttributeName);
                    break;
                case DataAggregationType.Minimum:
                    columnList += string.Format(SQLStatements.SQL_Action_MinimumAggregation,
                                                QuoteIdentifier(tableName, engine), QuoteIdentifier(annotation.ColumnName, engine), entityAttributeName);
                    break;
                case DataAggregationType.Maximum:
                    columnList += string.Format(SQLStatements.SQL_Action_MaximumAggregation,
                                                QuoteIdentifier(tableName, engine), QuoteIdentifier(annotation.ColumnName, engine), entityAttributeName);
                    break;
            }
        }

        #endregion
    }
}
