using System;
using Rochas.Data.Specification.Enums;
using Rochas.Data.Specification.Interfaces;

namespace Rochas.Data.Specification.Annotations
{
    public class RelatedEntityAttribute : Attribute, IRelatedEntity
    {
        public RelationCardinality Cardinality;
        public string ForeignKeyAttribute;
        public Type IntermediaryEntity = null;
        public string IntermediaryKeyAttribute = null;

        public RelationCardinality GetRelationCardinality()
        {
            return Cardinality;
        }

        public Type GetIntermediaryEntity()
        {
            return IntermediaryEntity;
        }

        public string GetIntermediaryKeyAttribute()
        {
            return IntermediaryKeyAttribute;
        }
    }
}
