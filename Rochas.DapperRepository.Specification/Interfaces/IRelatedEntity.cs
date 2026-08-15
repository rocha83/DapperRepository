using System;
using Rochas.Data.Specification.Enums;

namespace Rochas.Data.Specification.Interfaces
{
    interface IRelatedEntity
    {
        RelationCardinality GetRelationCardinality();
        Type GetIntermediaryEntity();
        string GetIntermediaryKeyAttribute();
    }
}
