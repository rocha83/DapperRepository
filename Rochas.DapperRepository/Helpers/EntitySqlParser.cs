﻿using System;
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
		public static string ParseEntity(object entity, DatabaseEngine engine, PersistenceAction persistenceAction, object filterEntity = null, int recordLimit = 0, bool filterConjunction = false, bool onlyListableAttributes = false, string showAttributes = null, string groupAttributes = null, string sortAttributes = null, bool orderDescending = false, bool readUncommited = false)
        {
            try
            {
                string sqlInstruction;
                string[] displayAttributes = new string[0];
                Dictionary<object, object> attributeColumnRelation;

                var entityType = entity.GetType();
                var nameSpacePrefix = entityType.Namespace.Substring(0, entityType.Namespace.IndexOf("."));

                var entityProps = entityType.GetProperties()
                                            .Where(p => !p.PropertyType.Namespace.StartsWith(nameSpacePrefix)).ToArray();

                // Model validation
                if (!EntityReflector.VerifyTableAnnotation(entityType))
                    throw new InvalidOperationException("Entity table annotation not found. Please review model definition.");

                if (EntityReflector.GetKeyColumn(entityProps) == null)
                    throw new KeyNotFoundException("Entity key column annotation not found. Please review model definition.");
                //

                if (onlyListableAttributes)
                    EntityReflector.ValidateListableAttributes(entityProps, showAttributes, out displayAttributes);

                sqlInstruction = GetSqlInstruction(entity, entityType, entityProps, engine, persistenceAction, filterEntity,
                                                   recordLimit, filterConjunction, displayAttributes, groupAttributes, readUncommited);

                if ((persistenceAction != PersistenceAction.Create) && (persistenceAction != PersistenceAction.Edit))
				{
                    sqlInstruction = string.Format(sqlInstruction, ((engine != DatabaseEngine.SQLServer) && (recordLimit > 0))
                                   ? string.Format(SQLStatements.SQL_Action_LimitResult_MySQL, recordLimit)
                                   : string.Empty, "{0}", "{1}");

                    attributeColumnRelation = EntityReflector.GetPropertiesValueList(entity, entityType, entityProps, persistenceAction);

                    if (!string.IsNullOrEmpty(groupAttributes))
                        ParseGroupingAttributes(attributeColumnRelation, groupAttributes, ref sqlInstruction);
                    else
                        sqlInstruction = string.Format(sqlInstruction, string.Empty, "{0}");

                    if (!string.IsNullOrEmpty(sortAttributes))
                        ParseOrdinationAttributes(attributeColumnRelation, sortAttributes, orderDescending, ref sqlInstruction);
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

        #endregion

        #region Helper Methods

        private static string GetSqlInstruction(object entity, Type entityType, PropertyInfo[] entityProps, DatabaseEngine engine, PersistenceAction action, object filterEntity, int recordLimit, bool filterConjunction, string[] showAttributes, string groupAttributes, bool readUncommited = false)
        {
            string sqlInstruction;
            Dictionary<object, object> sqlFilterData;
            Dictionary<string, object[]> rangeValues = null;
            Dictionary<object, object> sqlEntityData = EntityReflector.GetPropertiesValueList(entity, entityType, entityProps, action);

            if (filterEntity != null)
            {
                sqlFilterData = EntityReflector.GetPropertiesValueList(filterEntity, entityType, entityProps, action);
                rangeValues = EntityReflector.GetEntityRangeFilter(filterEntity, entityProps);
            }
            else
                sqlFilterData = null;

            var keyColumnName = EntityReflector.GetKeyColumnName(entityProps);

            Dictionary<string, string> sqlParameters = GetSqlParameters(sqlEntityData, engine, action, sqlFilterData,
                                                                        recordLimit, filterConjunction, showAttributes, 
                                                                        keyColumnName, rangeValues, groupAttributes, readUncommited);
            switch (action)
            {
                case PersistenceAction.Create:

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Create,
                                                   sqlParameters["TableName"],
                                                   sqlParameters["ColumnList"],
                                                   sqlParameters["ValueList"]);

                    break;

                case PersistenceAction.Edit:

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Edit,
                                                   sqlParameters["TableName"],
                                                   sqlParameters["ColumnValueList"],
                                                   sqlParameters["ColumnFilterList"]);

                    break;

                case PersistenceAction.Delete:

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Delete,
                                                   sqlParameters["TableName"],
                                                   sqlParameters["ColumnFilterList"]);

                    break;
                default: // Listagem ou Consulta

                    sqlInstruction = String.Format(SQLStatements.SQL_Action_Query,
                                                   sqlParameters["ColumnList"],
                                                   sqlParameters["TableName"],
                                                   sqlParameters["RelationList"],
                                                   sqlParameters["ColumnFilterList"],
                                                   "{0}", "{1}", "{2}");

                    break;
            }

            return sqlInstruction;
        }

        private static void ParseGroupingAttributes(Dictionary<object, object> attributeColumnRelation, string groupAttributes, ref string sqlInstruction)
        {
            string columnList = string.Empty;
            string complementaryColumnList = string.Empty;
            string[] groupingAttributes = groupAttributes.Split(',');

            for (int cont = 0; cont < groupingAttributes.Length; cont++)
                groupingAttributes[cont] = groupingAttributes[cont].Trim();

            foreach (var rel in attributeColumnRelation)
                if (Array.IndexOf(groupingAttributes, rel.Key) > -1)
                    columnList += string.Format("{0}, ", ((KeyValuePair<object, object>)rel.Value).Key);
                else
                    if (!rel.Key.Equals("TableName"))
                    complementaryColumnList += string.Format("{0}, ", ((KeyValuePair<object, object>)rel.Value).Key);

            if (!string.IsNullOrEmpty(columnList) && (columnList.Length > 2))
                columnList = columnList.Substring(0, columnList.Length - 2);
            if (!string.IsNullOrEmpty(complementaryColumnList) && (complementaryColumnList.Length > 2))
                complementaryColumnList = complementaryColumnList.Substring(0, complementaryColumnList.Length - 2);

            sqlInstruction = string.Format(sqlInstruction,
                                           string.Format(SQLStatements.SQL_Action_Group,
                                                         columnList, ", ", complementaryColumnList),
                                                         "{0}");
        }

        private static void ParseOrdinationAttributes(Dictionary<object, object> attributeColumnRelation, string sortAttributes, bool orderDescending, ref string sqlInstruction)
        {
            string columnList = string.Empty;
            string[] ordinationAttributes = sortAttributes.Split(',');

            for (int contAtrib = 0; contAtrib < ordinationAttributes.Length; contAtrib++)
            {
                ordinationAttributes[contAtrib] = ordinationAttributes[contAtrib].Trim();

                var attribToOrder = attributeColumnRelation.FirstOrDefault(rca => ordinationAttributes[contAtrib].Equals(rca.Key));
                var columnToOrder = ((KeyValuePair<object, object>)attribToOrder.Value).Key;

                if (!(columnToOrder is RelationalColumn))
                    columnList = string.Concat(columnList, columnToOrder, ", ");
                else
                    columnList = string.Concat(columnList, string.Format("{0}.{1}", ((RelationalColumn)columnToOrder).TableName.ToLower(),
                                                                                    ((RelationalColumn)columnToOrder).ColumnName), ", ");
            }

            columnList = columnList.Substring(0, columnList.Length - 2);

            sqlInstruction = string.Format(sqlInstruction,
                                           string.Format(SQLStatements.SQL_Action_OrderResult,
                                                         columnList,
                                                         orderDescending ? "DESC" : "ASC"));
        }

        private static Dictionary<string, string> GetSqlParameters(Dictionary<object, object> entitySqlData, DatabaseEngine engine, PersistenceAction action, IDictionary<object, object> entitySqlFilter, int recordLimit, bool filterConjunction, string[] showAttributes, string keyColumnName, IDictionary<string, object[]> rangeValues, string groupAttributes, bool readUncommited = false)
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
                            columnList += string.Format("{0}.{1}, ", tableName, entityColumnName);
                    }

                    if (item.Key.Equals("TableName"))
                    {
                        returnDictionary.Add(item.Key.ToString(), item.Value.ToString());
                        tableName = item.Value.ToString();
                    }
                    else if (itemChildKeyPair.Key is RelationalColumn)
                    {
                        SetRelationalSqlParameters(itemChildKeyPair, tableName, ref columnList, ref relationList);
                    }
                    else if (itemChildKeyPair.Key is DataAggregationColumn)
                    {
                        SetAggregationSqlParameters(itemChildKeyPair, tableName, entityAttributeName, ref columnList);
                    }
                    else
                    {
                        SetPredicateSqlParameters(itemChildKeyPair, engine, action, tableName, keyColumnName, entityColumnName, entityAttributeName,
                                                  recordLimit, showAttributes, ref columnList, ref valueList, ref columnValueList);
                    }
                }

            if (entitySqlFilter != null)
                SetFilterSqlParameters(entitySqlFilter, tableName, action, rangeValues, ref columnFilterList, filterConjunction);

            FillSqlParametersResult(returnDictionary, action, ref columnList, ref valueList, ref columnValueList, ref columnFilterList, ref relationList, readUncommited);

            return returnDictionary;
        }

        public static void ParseOneToManyRelation(object childEntityFilter, object listItem, Type listItemType, PropertyInfo[] listItemProps,
                                                  ref PersistenceAction action, List<object> childFiltersList)
        {
            childEntityFilter = Activator.CreateInstance(listItemType);

            action = SetPersistenceAction(listItem, EntityReflector.GetKeyColumn(listItemProps));

            if (action == PersistenceAction.Edit)
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
                    ? PersistenceAction.Create : PersistenceAction.Edit;
        }

        private static void FillSqlParametersResult(IDictionary<string, string> returnDictionary, PersistenceAction action, ref string columnList, ref string valueList, ref string columnValueList, ref string columnFilterList, ref string relationList, bool readUncommited = false)
        {
            if (action == PersistenceAction.Create)
            {
                columnList = columnList.Substring(0, columnList.Length - 2);
                valueList = valueList.Substring(0, valueList.Length - 2);

                returnDictionary.Add("ColumnList", columnList);
                returnDictionary.Add("ValueList", valueList);
            }
            else
            {
                if ((action == PersistenceAction.List)
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
                case PersistenceAction.Create:
                    columnList += string.Format("{0}, ", entityColumnName);
                    valueList += string.Format("{0}, ", entityColumnValue);

                    break;
                case PersistenceAction.List:

                    if ((engine == DatabaseEngine.SQLServer) && string.IsNullOrWhiteSpace(columnList) && (recordLimit > 0))
                        columnList += string.Format(SQLStatements.SQL_Action_LimitResult, recordLimit);

                    if (((showAttributes == null) || (showAttributes.Length == 0))
                        || showAttributes.Length > 0 && Array.IndexOf(showAttributes, entityAttributeName) > -1)
                    {
                        var columnAlias = isCustomColumn ? string.Format(" AS {0}", entityAttributeName) : string.Empty;
                        columnList += string.Format("{0}.{1}{2}, ", tableName, entityColumnName, columnAlias);
                    }

                    break;
                case PersistenceAction.Get:

                    if (((showAttributes == null) || showAttributes.Length == 0)
                        || showAttributes.Length > 0 && Array.IndexOf(showAttributes, entityAttributeName) > -1)
                    {
                        var columnAlias = isCustomColumn ? string.Format(" AS {0}", entityAttributeName) : string.Empty;
                        columnList += string.Format("{0}.{1}{2}, ", tableName, entityColumnName, columnAlias);
                    }

                    break;
                case PersistenceAction.Count:

                    if (entityColumnName.Equals(keyColumnName))
                        columnList += string.Format(SQLStatements.SQL_Action_CountAggregation,
                                                    tableName, entityColumnName, entityAttributeName);

                    break;
                default: // Alteração e Exclusão
                    if (!entityAttributeName.ToLower().Equals("id"))
                    {
                        if (entityColumnValue == null)
                            entityColumnValue = SqlDefaultValue.Null;

                        columnValueList += string.Format("{0} = {1}, ", entityColumnName, entityColumnValue);
                    }

                    break;
            }
        }

        private static void SetFilterSqlParameters(IDictionary<object, object> entitySqlFilter, string tableName, PersistenceAction action, IDictionary<string, object[]> rangeValues, ref string columnFilterList, bool filterConjunction)
        {
            foreach (var filter in entitySqlFilter)
            {
                if (!filter.Key.Equals("TableName") && !filter.Key.Equals("RelatedEntityAttribute"))
                {
                    object filterColumnName = null;
                    object filterColumnValue = null;
                    object columnName = null;
                    string columnNameStr = string.Empty;

                    var itemChildKeyPair = (KeyValuePair<object, object>)filter.Value;
                    if (!(itemChildKeyPair.Key is RelationalColumn))
                    {
                        columnName = itemChildKeyPair.Key;
                        filterColumnName = string.Concat(tableName, ".", columnName);
                        filterColumnValue = itemChildKeyPair.Value;
                    }
                    else
                    {
                        RelationalColumn relationConfig = itemChildKeyPair.Key as RelationalColumn;

                        if ((action == PersistenceAction.List) && relationConfig.Filterable)
                        {
                            filterColumnName = string.Concat(relationConfig.TableName.ToLower(), ".", relationConfig.ColumnName);
                            filterColumnValue = itemChildKeyPair.Value;
                        }
                    }

                    var rangeFilter = false;
                    if (rangeValues != null)
                    {
                        columnNameStr = columnName.ToString();
                        rangeFilter = rangeValues.ContainsKey(columnNameStr);
                    }

                    if (((filterColumnValue != null)
                            && (filterColumnValue.ToString() != SqlDefaultValue.Null)
                            && (filterColumnValue.ToString() != SqlDefaultValue.Zero))
                        || rangeFilter)
                    {
                        bool compareRule = ((action == PersistenceAction.List)
                                            || (action == PersistenceAction.Count))
                                         && !long.TryParse(filterColumnValue.ToString(), out long fake)
                                         && !filterColumnName.ToString().ToLower().Contains("date")
                                         && !filterColumnName.ToString().ToLower().StartsWith("id")
                                         && !filterColumnName.ToString().ToLower().EndsWith("id")
                                         && !filterColumnName.ToString().ToLower().Contains(".id");

                        string comparation = string.Empty;

                        var filterColumnValueStr = filterColumnValue.ToString()
                                                                    .Replace("(", string.Empty)
                                                                    .Replace(")", string.Empty);

						if (!rangeFilter)
                        {
                            comparation = (compareRule)
                                          ? string.Format(SqlOperator.Contains, filterColumnValueStr.Replace("'", string.Empty))
                                          : string.Concat(SqlOperator.Equal, filterColumnValueStr);

                            if (filterColumnValue.Equals(true))
                                comparation = " = 1";

                            if ((action == PersistenceAction.Edit) && filterColumnValue.Equals(false))
                                comparation = " = 0";

                            if (!filterColumnValue.Equals(false))
                                columnFilterList += filterColumnName + comparation +
                                    ((compareRule && !filterConjunction) ? SqlOperator.Or : SqlOperator.And);
                        }
                        else
                            SetRangeFilterSql(filter, rangeValues, columnNameStr, 
                                              filterColumnName.ToString(), ref columnFilterList);
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
                                            ref string columnFilterList)
        {
            string rangeFrom = "'{0}'";
            string rangeTo = "'{0}'";
            string comparation = string.Empty;

            var isNumericRange = double.TryParse(rangeValues[columnNameStr][0].ToString(), out var fake1);
            var isDateRange = filter.Key.ToString().ToLower().Contains("date");

            if (isNumericRange)
                comparation = GetNumericRangeComparation(rangeValues, columnNameStr, ref rangeFrom, ref rangeTo);
            else if (isDateRange)
                comparation = GetDateRangeComparation(rangeValues, columnNameStr, ref rangeFrom, ref rangeTo);
            
            if (!string.IsNullOrWhiteSpace(comparation))
                columnFilterList += string.Concat(columnNameStr, " ", comparation, SqlOperator.And);
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
            var emptyRangeFrom = (DateTime)rangeValues[columnNameStr][0] == DateTime.MinValue;
            var emptyRangeTo = (DateTime)rangeValues[columnNameStr][1] == DateTime.MinValue;

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

        private static void SetRelationalSqlParameters(KeyValuePair<object, object> itemChildKeyPair, string tableName, ref string columnList, ref string relationList)
        {
            string relation;
            RelationalColumn relationConfig = itemChildKeyPair.Key as RelationalColumn;

            columnList += string.Format("{0}.{1} ", relationConfig.TableName.ToLower(), relationConfig.ColumnName);

            if (!string.IsNullOrEmpty(relationConfig.ColumnAlias))
                columnList += string.Format(SQLStatements.SQL_Action_ColumnAlias, relationConfig.ColumnAlias);

            columnList += ", ";

            if (relationConfig.JunctionType == RelationalJunctionType.Mandatory)
            {
                relation = string.Format(SQLStatements.SQL_Action_RelationateMandatorily,
                                                       relationConfig.TableName.ToLower(),
                                                       string.Concat(tableName, ".", relationConfig.KeyColumn),
                                                       string.Concat(relationConfig.TableName, ".",
                                                       relationConfig.ForeignKeyColumn, " "));
            }
            else
            {
                if (!string.IsNullOrEmpty(relationConfig.IntermediaryColumnName))
                {
                    relation = string.Format(SQLStatements.SQL_Action_RelationateOptionally,
                                             relationConfig.IntermediaryColumnName.ToLower(),
                                             string.Concat(tableName, ".", relationConfig.ForeignKeyColumn),
                                             string.Concat(relationConfig.IntermediaryColumnName, ".",
                                             relationConfig.ForeignKeyColumn));

                    relation += string.Format(SQLStatements.SQL_Action_RelationateOptionally,
                                              relationConfig.TableName,
                                              string.Concat(relationConfig.IntermediaryColumnName, ".", relationConfig.KeyColumn),
                                              string.Concat(relationConfig.TableName, ".", relationConfig.ForeignKeyColumn, " "));
                }
                else
                {
                    relation = string.Format(SQLStatements.SQL_Action_RelationateOptionally,
                                             relationConfig.TableName,
                                             string.Concat(tableName, ".", relationConfig.KeyColumn),
                                             string.Concat(relationConfig.TableName, ".", relationConfig.ForeignKeyColumn));
                }
            }

            if (relation.Contains(relationList)
                || string.IsNullOrEmpty(relationList))
                relationList = relation;
            else if (!relationList.Contains(relation))
                relationList += relation;
        }

        private static void SetAggregationSqlParameters(KeyValuePair<object, object> itemChildKeyPair, string tableName, string entityAttributeName, ref string columnList)
        {
            var annotation = itemChildKeyPair.Key as DataAggregationColumn;

            switch (annotation.AggregationType)
            {
                case DataAggregationType.Count:
                    columnList += string.Format(SQLStatements.SQL_Action_CountAggregation,
                                                tableName, annotation.ColumnName, entityAttributeName);
                    break;
                case DataAggregationType.Sum:
                    columnList += string.Format(SQLStatements.SQL_Action_SummaryAggregation,
                                                tableName, annotation.ColumnName, entityAttributeName);
                    break;
                case DataAggregationType.Average:
                    columnList += string.Format(SQLStatements.SQL_Action_AverageAggregation,
                                                tableName, annotation.ColumnName, entityAttributeName);
                    break;
                case DataAggregationType.Minimum:
                    columnList += string.Format(SQLStatements.SQL_Action_MinimumAggregation,
                                                tableName, annotation.ColumnName, entityAttributeName);
                    break;
                case DataAggregationType.Maximum:
                    columnList += string.Format(SQLStatements.SQL_Action_MaximumAggregation,
                                                tableName, annotation.ColumnName, entityAttributeName);
                    break;
            }
        }

        #endregion
    }
}
