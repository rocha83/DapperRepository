using System;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Specification.Interfaces;

namespace Rochas.DapperRepository.Specification.Annotations
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
