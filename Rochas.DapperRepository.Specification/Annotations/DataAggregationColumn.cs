using System;
using Rochas.Data.Specification.Enums;

namespace Rochas.Data.Specification.Annotations
{
    public class DataAggregationColumn : Attribute
    {
        public string ColumnName;
        public DataAggregationType AggregationType;
    }
}
