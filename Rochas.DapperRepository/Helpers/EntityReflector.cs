﻿using Rochas.DapperRepository.Specification.Annotations;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Exceptions;
using Rochas.DapperRepository.Helpers.SQL;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Data;
using System.Collections;

namespace Rochas.DapperRepository.Helpers
{
    public static class EntityReflector
    {
        public static Dictionary<object, object> GetPropertiesValueList(object entity, Type entityType, PropertyInfo[] entityProperties, PersistenceAction action)
        {
            var objectSQLDataRelation = new Dictionary<object, object>();

            var tableAnnotation = GetTableAttribute(entityType);
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
                var isRelatedEntity = prop.GetCustomAttributes().Any(atb => atb is RelatedEntityAttribute);
                if (!isRelatedEntity)
                {
                    var isAutoGenerated = prop.GetCustomAttributes().Any(atb => atb is AutoGeneratedAttribute);
                    var isNotMapped = prop.GetCustomAttributes().Any(atb => atb is NotMappedAttribute);
                    var columnAnnotation = prop.GetCustomAttribute(typeof(ColumnAttribute)) as ColumnAttribute;
                    var columnName = prop.Name;
                    if (columnAnnotation != null)
                        columnName = columnAnnotation.Name;

                    var columnValue = prop.GetValue(Convert.ChangeType(entity, entity.GetType()), null);
                    columnValue = FormatSQLInputValue(prop, columnValue, action);

                    if (!((action == PersistenceAction.Create) && isAutoGenerated) && !isNotMapped)
                    {
                        var sqlValueColumn = new KeyValuePair<object, object>(columnName, columnValue);
                        objectSQLDataRelation.Add(prop.Name, sqlValueColumn);
                    }
                }
            }

            return objectSQLDataRelation;
        }

        public static void ValidateListableAttributes(PropertyInfo[] entityProps, string showProperties, out string[] exibitionAttributes)
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

        private static TableAttribute GetTableAttribute(Type entityType)
        {
            return entityType.GetCustomAttribute(typeof(TableAttribute)) as TableAttribute;
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
                        if (((int)columnValue == int.MinValue) && !isRequired)
                            columnValue = SqlDefaultValue.Null;
                        break;
                    case SQL.DataType.Long:
                        if (((long)columnValue == long.MinValue) && !isRequired)
                            columnValue = SqlDefaultValue.Null;
                        break;
                    case SQL.DataType.Boolean:
                        columnValue = ((bool)columnValue) ? 1 : 0;
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
                        {
                            var dateFormat = (action == PersistenceAction.Create) ?
                                              DateTimeFormat.CompleteDateTime :
                                              DateTimeFormat.NormalDate;

                            columnValue = "'" + ((DateTime)columnValue).ToString(dateFormat) + "'";
                        }
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
                    case SQL.DataType.Decimal:
                        if (((decimal)columnValue == 0) && !isRequired)
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

        public static PropertyInfo GetKeyColumn(PropertyInfo[] entityProps)
        {
            return entityProps?.FirstOrDefault(prp => prp.GetCustomAttributes().Any(atb => atb is KeyAttribute));
        }

        public static object GetFilterByPrimaryKey(Type entityType, PropertyInfo[] entityProps, object key)
        {
            var entity = Activator.CreateInstance(entityType);
            if (entity != null)
            {
                var keyColumn = GetKeyColumn(entityProps);
                keyColumn?.SetValue(entity, key);
            }

            return entity;
        }

        public static object GetFilterByFilterableColumns(Type entityType, PropertyInfo[] entityProps, object criteria)
        {
            var entity = Activator.CreateInstance(entityType);
            if (entity != null)
            {
                var filterableProps = entityProps?.Where(prp => prp.GetCustomAttribute(typeof(FilterableAttribute)) != null);
                if (filterableProps != null)
                    foreach (var prop in filterableProps)
                        prop.SetValue(entity, criteria);
            }

            return entity;
        }

        public static void SetFilterPrimaryKey(object entity, PropertyInfo[] entityProps, object filterEntity)
        {
            var entityKeyColumn = GetKeyColumn(entityProps);

            if (entityKeyColumn != null)
            {
                var keyValue = entityKeyColumn.GetValue(entity, null);
                entityKeyColumn.SetValue(filterEntity, keyValue, null);
                entityKeyColumn.SetValue(entity, 0, null);
            }
        }

        public static void SetChildForeignKeyValue(object parentEntity, PropertyInfo[] parentProps, object childEntity, 
                                                   PropertyInfo[] childProps, string foreignKeyPropName)
        {
            var parentKey = GetKeyColumn(parentProps);
            var childForeignKey = childProps?.FirstOrDefault(prp => prp.Name.Equals(foreignKeyPropName));

            if ((parentKey != null) && (childForeignKey != null))
                childForeignKey.SetValue(childEntity, parentKey.GetValue(parentEntity, null), null);
        }

        public static void SetParentForeignKeyValue(object parentEntity, PropertyInfo[] parentProps, object childEntity, 
                                                    PropertyInfo[] childProps, string foreignKeyPropName)
        {
            var parentForeignKey = parentProps?.FirstOrDefault(prp => prp.Name.Equals(foreignKeyPropName));
            var childKey = GetKeyColumn(childProps);

            if ((parentForeignKey != null) && (childKey != null))
                childKey.SetValue(childEntity, parentForeignKey.GetValue(parentEntity, null), null);
        }

        public static IEnumerable<PropertyInfo> GetRelatedEntities(PropertyInfo[] entityProperties)
        {
             return entityProperties.Where(prp => prp.GetCustomAttributes(true)
                                    .Any(atb => atb.GetType().Name.Equals("RelatedEntityAttribute")));
        }

        public static RelatedEntityAttribute GetRelatedEntityAttribute(PropertyInfo relatedEntity)
        {
            return relatedEntity.GetCustomAttributes(true)
                                .FirstOrDefault(atb => atb.GetType().Name.Equals("RelatedEntityAttribute")) as RelatedEntityAttribute;
        }

        public static bool MatchKeys(object sourceEntity, PropertyInfo[] entityProps, object destinEntity)
        {
            var entityKey = GetKeyColumn(entityProps);

            return entityKey.GetValue(sourceEntity, null)
                   .Equals(entityKey.GetValue(destinEntity, null));
        }

        public static void SetEntityForeignKey(object parentEntity, object childEntity)
        {
            var parentProps = parentEntity.GetType().GetProperties();
            var parentKey = GetKeyColumn(parentProps);

            var childProps = childEntity.GetType().GetProperties();
            var childForeignKey = GetKeyColumn(childProps);

            if ((parentKey != null) && (childForeignKey != null))
                childForeignKey.SetValue(childEntity, parentKey.GetValue(parentEntity, null), null);
        }

        public static string GetKeyColumnName(PropertyInfo[] entityProps)
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

        public static Dictionary<string, object[]> GetEntityRangeFilter(object entity, PropertyInfo[] entityProps)
        {
            Dictionary<string, object[]> result = null; 

            var rangeFilterProps =  entityProps.Where(prp => prp.GetCustomAttributes(true)
                                                 .Any(atb => atb.GetType().Name.Equals("RangeFilterAttribute")
                                                      && !string.IsNullOrWhiteSpace(((RangeFilterAttribute)atb).LinkedRangeProperty)));

            if (rangeFilterProps != null)
            {
                result = new Dictionary<string, object[]>();
                foreach (var prop in rangeFilterProps)
                {
                    var columnAnnotation = prop.GetCustomAttribute(typeof(ColumnAttribute)) as ColumnAttribute;
                    if (columnAnnotation != null)
                    {
                        var columnName = columnAnnotation.Name;
                        var rangePropAttrib = prop.GetCustomAttributes(true)
                                                  .FirstOrDefault(lkp => lkp.GetType().Name.Equals("RangeFilterAttribute"));

                        var linkedRangePropName = ((RangeFilterAttribute)rangePropAttrib).LinkedRangeProperty;
                        var lkdRangeProp = entityProps.FirstOrDefault(lkp => lkp.Name.Equals(linkedRangePropName));
                        if (lkdRangeProp != null)
                        {
                            result.Add(columnName,
                                new object[] { prop.GetValue(entity), lkdRangeProp.GetValue(entity) });
                        }
                        else
                            throw new Exception("Linked range property not found.");
                    }
                }
            }

            return result;
        }

        public static IList CreateTypedList(PropertyInfo child)
        {
            var childListType = child.PropertyType.GetGenericArguments()[0];
            var dynamicManyType = typeof(List<>).MakeGenericType(new Type[] { childListType });
            return (IList)Activator.CreateInstance(dynamicManyType, true);
        }

        public static bool VerifyTableAnnotation(Type entityType)
        {
            return (GetTableAttribute(entityType) != null);
        }

        public static DataTable GetDataTable<T>(ICollection<T> list)
        {
            DataTable table = CreateTable<T>();
            var properties = typeof(T).GetProperties();
            foreach (var item in list)
            {
                DataRow row = table.NewRow();
                foreach (var prop in properties)
                {
                    row[prop.Name] = prop.GetValue(item);
                }
                table.Rows.Add(row);
            }
            return table;
        }
        private static DataTable CreateTable<T>()
        {
            Type entityType = typeof(T);
            DataTable table = new DataTable(entityType.Name);
            var properties = entityType.GetProperties();
            foreach (var prop in properties)
            {
                table.Columns.Add(prop.Name, prop.PropertyType);
            }
            return table;
        }
    }
}
