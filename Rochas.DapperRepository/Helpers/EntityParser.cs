﻿using System;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Rochas.DapperRepository.Annotations;
using Rochas.DapperRepository.Enums;
using Rochas.DapperRepository.Exceptions;
using Rochas.DapperRepository.Helpers.SQL;

namespace Rochas.DapperRepository.Helpers
{
    public static class EntitySqlParser
    {
        #region Public Methods

        public static string ParseEntity(object entity, PersistenceAction persistenceAction, object filterEntity = null, int recordLimit = 0, bool onlyListableAttributes = false, string showAttributes = null, IDictionary<string, double[]> rangeValues = null, string groupAttributes = null, string orderAttributes = null, bool orderDescending = false, bool readUncommited = true)
        {
            try
            {
                string sqlInstruction;
                string[] displayAttributes = new string[0];
                Dictionary<object, object> attributeColumnRelation;

                var entityProps = entity.GetType().GetProperties();

                // Model validation
                if (!VerifyTableAnnotation(entity))
                    throw new InvalidOperationException("Entity table annotation not found. Please review model definition.");

                if (GetKeyColumn(entityProps) == null)
                    throw new KeyNotFoundException("Entity key column annotation not found. Please review model definition.");
                //

                if (onlyListableAttributes)
                    ValidateListableAttributes(entityProps, showAttributes, out displayAttributes);

                sqlInstruction = GetSqlInstruction(entity, entityProps, persistenceAction, filterEntity,
                                                   displayAttributes, rangeValues, groupAttributes);

                sqlInstruction = string.Format(sqlInstruction, recordLimit > 0
                               ? string.Format(SQLStatements.SQL_Action_LimitResult_MySQL, recordLimit)
                               : string.Empty, "{0}", "{1}");

                attributeColumnRelation = GetPropertiesValueList(entity, entityProps, persistenceAction);

                if (!string.IsNullOrEmpty(groupAttributes))
                    ParseGroupingAttributes(attributeColumnRelation, groupAttributes, ref sqlInstruction);
                else
                    sqlInstruction = string.Format(sqlInstruction, string.Empty, "{0}");

                if (!string.IsNullOrEmpty(orderAttributes))
                    ParseOrdinationAttributes(attributeColumnRelation, orderAttributes, orderDescending, ref sqlInstruction);
                else
                    sqlInstruction = string.Format(sqlInstruction, string.Empty);

                return sqlInstruction;
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }

        public static Dictionary<object, object> GetPropertiesValueList(object entity, PropertyInfo[] entityProperties, PersistenceAction action)
        {
            var objectSQLDataRelation = new Dictionary<object, object>();

            var tableAnnotation = entity.GetType().GetCustomAttribute(typeof(TableAttribute)) as TableAttribute;
            if (tableAnnotation != null)
            {
                var tableName = string.Empty;

                if (!string.IsNullOrWhiteSpace(tableAnnotation.Schema))
                    tableName = string.Format("{0}.{1}", tableAnnotation.Schema, tableAnnotation.Name);
                else
                    tableName = string.Format("{0}", tableAnnotation.Name);

                objectSQLDataRelation.Add("TableName", tableName);
            }

            foreach (var prop in entityProperties)
            {
                var isAutoGenerated = prop.GetCustomAttributes().Any(atb => atb is AutoGeneratedAttribute);
                var columnAnnotation = prop.GetCustomAttribute(typeof(ColumnAttribute)) as ColumnAttribute;
                var columnName = prop.Name;
                if (columnAnnotation != null)
                    columnName = columnAnnotation.Name;

                var columnValue = prop.GetValue(Convert.ChangeType(entity, entity.GetType()), null);
                columnValue = FormatSQLInputValue(prop, columnValue, action);

                if (!((action == PersistenceAction.Create) && isAutoGenerated))
                {
                    var sqlValueColumn = new KeyValuePair<object, object>(columnName, columnValue);
                    objectSQLDataRelation.Add(prop.Name, sqlValueColumn);
                }
            }

            return objectSQLDataRelation;
        }

        #endregion

        #region Helper Methods

        private static string GetSqlInstruction(object entity, PropertyInfo[] entityProps, PersistenceAction action, object filterEntity, string[] showAttributes, IDictionary<string, double[]> rangeValues, string groupAttributes, bool readUncommited = true)
        {
            string sqlInstruction;
            Dictionary<object, object> sqlFilterData;
            Dictionary<object, object> sqlEntityData = GetPropertiesValueList(entity, entityProps, action);

            if (filterEntity != null)
                sqlFilterData = GetPropertiesValueList(filterEntity, entityProps, action);
            else
                sqlFilterData = null;

            var keyColumnName = GetKeyColumnName(entityProps);

            Dictionary<string, string> sqlParameters = GetSqlParameters(sqlEntityData, action, sqlFilterData,
                                                                        showAttributes, keyColumnName,
                                                                        rangeValues, groupAttributes);
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
                                                   "{0}", "{1}", string.Empty);

                    break;
            }

            GC.Collect(2);

            return sqlInstruction;
        }
        
        private static void ValidateListableAttributes(PropertyInfo[] entityProps, string showProperties, out string[] exibitionAttributes)
        {
            IEnumerable<PropertyInfo> listableAttributes = null;
            bool notListableAttribute = false;

            listableAttributes = entityProps.Where(prp => prp.GetCustomAttributes(true).
                                             Where(ca => (ca is ListableAttribute)).Any());

            if (string.IsNullOrWhiteSpace(showProperties))
            {
                exibitionAttributes = new string[listableAttributes.Count()];

                int cont = 0;
                foreach (var listableAttrib in listableAttributes)
                {
                    exibitionAttributes[cont] = listableAttrib.Name;
                    cont++;
                }
            }
            else
            {
                exibitionAttributes = showProperties.Split(',');

                foreach (var attrib in exibitionAttributes)
                {
                    notListableAttribute = !listableAttributes.Contains(attrib.GetType().GetProperty(attrib));
                    if (!notListableAttribute) throw new PropertyNotListableException(attrib); break;
                }
            }
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

        private static void ParseOrdinationAttributes(Dictionary<object, object> attributeColumnRelation, string orderAttributes, bool orderDescending, ref string sqlInstruction)
        {
            string columnList = string.Empty;
            string[] ordinationAttributes = orderAttributes.Split(',');

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

        public static PropertyInfo GetKeyColumn(PropertyInfo[] entityProps)
        {
            return entityProps.FirstOrDefault(prp => prp.GetCustomAttributes().Any(atb => atb is KeyAttribute));
        }

        public static PropertyInfo GetForeignKeyColumn(PropertyInfo[] entityProps)
        {
            return entityProps.FirstOrDefault(prp => prp.GetCustomAttributes().Any(atb => atb is ForeignKeyAttribute));
        }

        public static void MigrateEntityPrimaryKey(object entity, object filterEntity)
        {
            var entityProps = entity.GetType().GetProperties();
            var entityKeyColumn = GetKeyColumn(entityProps);

            if (entityKeyColumn != null)
            {
                var keyValue = entityKeyColumn.GetValue(entity, null);
                entityKeyColumn.SetValue(filterEntity, keyValue, null);
                entityKeyColumn.SetValue(entity, 0, null);
            }
        }

        public static bool MatchKeys(object sourceEntity, object destinEntity)
        {
            var entityProps = sourceEntity.GetType().GetProperties();
            var entityKey = GetKeyColumn(entityProps);

            return entityKey.GetValue(sourceEntity, null)
                   .Equals(entityKey.GetValue(destinEntity, null));
        }

        private static void SetEntityForeignKey(object parentEntity, object childEntity)
        {
            var parentProps = parentEntity.GetType().GetProperties();
            var parentKey = GetKeyColumn(parentProps);

            var childProps = childEntity.GetType().GetProperties();
            var childForeignKey = GetKeyColumn(childProps);

            if ((parentKey != null) && (childForeignKey != null))
                childForeignKey.SetValue(childEntity, parentKey.GetValue(parentEntity, null), null);
        }

        private static string GetKeyColumnName(PropertyInfo[] entityProps)
        {
            var result = string.Empty;
            var keyProperty = GetKeyColumn(entityProps);
            if (keyProperty != null)
            {
                var columnAnnotation = keyProperty.GetCustomAttribute(typeof(ColumnAttribute)) as ColumnAttribute;
                if (columnAnnotation != null)
                    result = columnAnnotation.Name;
                else
                    result = keyProperty.Name;
            }

            return result;
        }

        private static bool VerifyTableAnnotation(object entity)
        {
            return (entity.GetType().GetCustomAttribute(typeof(TableAttribute)) != null);
        }

        private static Dictionary<string, string> GetSqlParameters(Dictionary<object, object> entitySqlData, PersistenceAction action, IDictionary<object, object> entitySqlFilter, string[] showAttributes, string keyColumnName, IDictionary<string, double[]> rangeValues, string groupAttributes, bool readUncommited = true)
        {
            var returnDictionary = new Dictionary<string, string>();
            var relationshipDictionary = new Dictionary<string, string>();

            string tableName = string.Empty;
            string columnList = string.Empty;
            string valueList = string.Empty;
            string columnValueList = string.Empty;
            string columnFilterList = string.Empty;
            string relationList = string.Empty;
            string relation = string.Empty;
            bool rangeFilter = false;

            string entityAttributeName = string.Empty;
            string entityColumnName = string.Empty;
            bool isCustomColumn = false;

            if (entitySqlData != null)
                foreach (var item in entitySqlData)
                {
                    relation = string.Empty;

                    if (!item.Key.Equals("TableName"))
                    {
                        entityAttributeName = item.Key.ToString();
                        entityColumnName = ((KeyValuePair<object, object>)item.Value).Key.ToString();

                        if (!string.IsNullOrWhiteSpace(groupAttributes) && groupAttributes.Contains(entityAttributeName))
                            columnList += string.Format("{0}.{1}, ", tableName, entityColumnName);
                    }

                    if (item.Key.Equals("TableName"))
                    {
                        returnDictionary.Add(item.Key.ToString(), item.Value.ToString());
                        tableName = item.Value.ToString();
                    }
                    else if (((KeyValuePair<object, object>)item.Value).Key is RelationalColumn)
                    {
                        RelationalColumn relationConfig = ((KeyValuePair<object, object>)item.Value).Key as RelationalColumn;

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
                    else if (((KeyValuePair<object, object>)item.Value).Key is DataAggregationColumn)
                    {
                        var annotation = ((KeyValuePair<object, object>)item.Value).Key as DataAggregationColumn;

                        if ((action == PersistenceAction.Max) && (annotation.AggregationType == DataAggregationType.Maximum))
                            columnList += string.Format(SQLStatements.SQL_Action_MaximumAggregation,
                                                        tableName, annotation.ColumnName, entityAttributeName);
                        else if ((action == PersistenceAction.Count) && (annotation.AggregationType == DataAggregationType.Count))
                            columnList += string.Format(SQLStatements.SQL_Action_CountAggregation,
                                                        tableName, annotation.ColumnName, entityAttributeName);
                    }
                    else
                    {
                        object entityColumnValue = ((KeyValuePair<object, object>)item.Value).Value;
                        isCustomColumn = !entityAttributeName.Equals(entityColumnName);

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
                                
                                if (((showAttributes == null) || (showAttributes.Length == 0))
                                    || showAttributes.Length > 0 && Array.IndexOf(showAttributes, entityAttributeName) > -1)
                                {
                                    var columnAlias = isCustomColumn ? string.Format(" AS {0}", entityAttributeName) : string.Empty;
                                    columnList += string.Format("{0}.{1}{2}, ", tableName, entityColumnName, columnAlias);
                                }

                                break;
                            case PersistenceAction.Get:
                                
                                if ((showAttributes.Length == 0)
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
                }

            if (entitySqlFilter != null)
            {
                foreach (var filter in entitySqlFilter)
                {
                    if (!filter.Key.Equals("TableName") && !filter.Key.Equals("RelatedEntity"))
                    {
                        object filterColumnName = null;
                        object filterColumnValue = null;
                        object columnName = null;
                        string columnNameStr = string.Empty;

                        if (!(((KeyValuePair<object, object>)filter.Value).Key is RelationalColumn))
                        {
                            columnName = ((KeyValuePair<object, object>)filter.Value).Key;
                            filterColumnName = string.Concat(tableName, ".", columnName);
                            filterColumnValue = ((KeyValuePair<object, object>)filter.Value).Value;
                        }
                        else
                        {
                            RelationalColumn relationConfig = ((KeyValuePair<object, object>)filter.Value).Key as RelationalColumn;

                            if ((action == PersistenceAction.List) && relationConfig.Filterable)
                            {
                                filterColumnName = string.Concat(relationConfig.TableName.ToLower(), ".", relationConfig.ColumnName);
                                filterColumnValue = ((KeyValuePair<object, object>)filter.Value).Value;
                            }
                        }

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
                            long fake;
                            bool compareRule = (action == PersistenceAction.List)
                                             && !long.TryParse(filterColumnValue.ToString(), out fake)
                                             && !filterColumnName.ToString().ToLower().Contains("date")
                                             && !filterColumnName.ToString().ToLower().Contains("hash")
                                             && !filterColumnName.ToString().ToLower().StartsWith("id")
                                             && !filterColumnName.ToString().ToLower().EndsWith("id")
                                             && !filterColumnName.ToString().ToLower().Contains(".id");

                            string comparation = string.Empty;

                            if (!rangeFilter)
                            {
                                comparation = (compareRule)
                                              ? string.Format(SqlOperator.Contains, filterColumnValue.ToString().Replace("'", string.Empty))
                                              : string.Concat(SqlOperator.Equal, filterColumnValue);

                                if (filterColumnValue.Equals(true))
                                    comparation = " = 1";

                                if ((action == PersistenceAction.Edit) && filterColumnValue.Equals(false))
                                    comparation = " = 0";

                                if (!filterColumnValue.Equals(false))
                                    columnFilterList += filterColumnName + comparation +
                                        ((compareRule) ? SqlOperator.Or : SqlOperator.And);
                            }
                            else
                            {
                                double rangeFrom = rangeValues[columnNameStr][0];
                                double rangeTo = rangeValues[columnNameStr][1];

                                comparation = string.Format(SqlOperator.Between, rangeFrom, rangeTo);

                                columnFilterList += string.Concat(filterColumnName, " ", comparation, SqlOperator.And);
                            }
                        }
                    }
                }
            }

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
                    || (action == PersistenceAction.Max)
                    || (action == PersistenceAction.Count))
                {
                    columnList = columnList.Substring(0, columnList.Length - 2);
                    returnDictionary.Add("ColumnList", columnList);
                    returnDictionary.Add("RelationList", relationList);

                    if (readUncommited)
                        returnDictionary["TableName"] = string.Concat(returnDictionary["TableName"], " (NOLOCK)");
                }
                else if (!string.IsNullOrEmpty(columnValueList))
                {
                    columnValueList = columnValueList.Substring(0, columnValueList.Length - 2);
                    returnDictionary.Add("ColumnValueList", columnValueList);
                }

                if (!string.IsNullOrEmpty(columnFilterList))
                {
                    var tokenRemove = (action == PersistenceAction.List)
                                       ? SqlOperator.Or.Length
                                       : SqlOperator.And.Length;

                    columnFilterList = columnFilterList.Substring(0, columnFilterList.Length - tokenRemove);

                    returnDictionary.Add("ColumnFilterList", columnFilterList);
                }
                else
                    returnDictionary.Add("ColumnFilterList", "1 = 1");
            }

            return returnDictionary;
        }

        public static object ParseManyToRelation(object childEntity, RelatedEntity relation)
        {
            object result = null;
            var relEntity = relation.IntermediaryEntity;

            if (relEntity != null)
            {
                var interEntity = Activator.CreateInstance(relation.IntermediaryEntity);

                var childProps = childEntity.GetType().GetProperties();
                var childKey = GetKeyColumn(childProps);
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
        private static object FormatSQLInputValue(PropertyInfo column, object columnValue, PersistenceAction action)
        {
            if (columnValue != null)
            {
                var isRequired = column.GetCustomAttributes().Any(atb => atb is RequiredAttribute);

                switch (columnValue.GetType().ToString())
                {
                    case SQL.DataType.Short:
                        if (((short)columnValue == short.MinValue) && !isRequired)
                            columnValue = SqlDefaultValue.Null;
                        break;
                    case SQL.DataType.Integer:
                        if ((((int)columnValue == int.MinValue) && !isRequired) || (action == PersistenceAction.List))
                            columnValue = SqlDefaultValue.Null;
                        break;
                    case SQL.DataType.Long:
                        if (((long)columnValue == long.MinValue) && !isRequired)
                            columnValue = SqlDefaultValue.Null;
                        break;
                    case SQL.DataType.String:
                        var strValue = columnValue.ToString().Replace("'", "\"");
                        if (!string.IsNullOrWhiteSpace(strValue))
                            columnValue = string.Concat("'", strValue, "'");
                        else
                            columnValue = SqlDefaultValue.Null;

                        break;
                    case SQL.DataType.DateTime:
                        if (!(((DateTime)columnValue).Equals(DateTime.MinValue)))
                            columnValue = "'" + ((DateTime)columnValue).ToString(DateTimeFormat.CompleteDateTime) + "'";
                        else
                            columnValue = SqlDefaultValue.Null;
                        break;
                    case SQL.DataType.Float:
                        if (((float)columnValue == 0) && !isRequired)
                            columnValue = SqlDefaultValue.Null;
                        else
                            columnValue = columnValue.ToString().Replace(",", ".");

                        break;
                    case SQL.DataType.Double:
                        if (((double)columnValue == 0) && !isRequired)
                            columnValue = SqlDefaultValue.Null;
                        else
                            columnValue = columnValue.ToString().Replace(",", ".");

                        break;
                }
            }
            else
            {
                columnValue = (action == PersistenceAction.Create) ? SqlDefaultValue.Null : null;
            }

            return columnValue;
        }

        #endregion
    }
}
