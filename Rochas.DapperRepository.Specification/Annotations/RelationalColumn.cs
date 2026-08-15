using System;
using Rochas.Data.Specification.Enums;

namespace Rochas.Data.Specification.Annotations
{
    public class RelationalColumn : Attribute
    {
        #region Declarations

        public string TableName;
        public string IntermediaryColumnName;
        public string ColumnName;
        public string ColumnAlias;
        public string KeyColumn;
        public string ForeignKeyColumn;
        public string IntermediaryColumnKey;
        public RelationalJunctionType JunctionType;
        public bool Filterable;

        public string GetColumnName()
        {
            return ColumnName;
        }

        #endregion
    }
}
