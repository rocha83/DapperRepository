﻿using Rochas.DapperRepository.Annotations;
using Rochas.DapperRepository.Enums;
using Rochas.DapperRepository.Exceptions;
using Rochas.DapperRepository.Helpers.SQL;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Text;

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

        public static PropertyInfo GetKeyColumn(PropertyInfo[] entityProps)
        {
            return entityProps?.FirstOrDefault(prp => prp.GetCustomAttributes().Any(atb => atb is KeyAttribute));
        }

        public static PropertyInfo[] GetFilterableColumns(PropertyInfo[] entityProps)
        {
            return entityProps?.Where(prp => prp.GetCustomAttribute(typeof(FilterableAttribute)) != null).ToArray();
        }

        public static PropertyInfo GetForeignKeyColumn(PropertyInfo[] entityProps)
        {
            return entityProps?.FirstOrDefault(prp => prp.GetCustomAttributes().Any(atb => atb is ForeignKeyAttribute));
        }

        public static void MigrateEntityPrimaryKey(object entity, PropertyInfo[] entityProps, object filterEntity)
        {
            var entityKeyColumn = GetKeyColumn(entityProps);

            if (entityKeyColumn != null)
            {
                var keyValue = entityKeyColumn.GetValue(entity, null);
                entityKeyColumn.SetValue(filterEntity, keyValue, null);
                entityKeyColumn.SetValue(entity, 0, null);
            }
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

        public static bool VerifyTableAnnotation(Type entityType)
        {
            return (GetTableAttribute(entityType) != null);
        }
    }
}
