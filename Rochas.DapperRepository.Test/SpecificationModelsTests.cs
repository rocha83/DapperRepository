using System;
using System.Collections.Generic;
using Xunit;
using Rochas.Data.Specification.Annotations;
using Rochas.Data.Specification.Enums;
using Rochas.Data.Specification.Models;

namespace Rochas.DapperRepository.Test
{
    public class SpecificationModelsTests
    {
        #region PaginatedResult

        [Fact]
        public void PaginatedResult_DefaultConstructor_InitializesItems()
        {
            var result = new PaginatedResult<int>();
            Assert.NotNull(result.Items);
            Assert.Empty(result.Items);
            Assert.Equal(0, result.PageCount);
        }

        [Fact]
        public void PaginatedResult_ParameterizedConstructor_SetsValues()
        {
            var items = new List<int> { 1, 2, 3 };
            var result = new PaginatedResult<int>(items, 100, 3, 25);

            Assert.Same(items, result.Items);
            Assert.Equal(100, result.TotalCount);
            Assert.Equal(3, result.Page);
            Assert.Equal(25, result.PageSize);
            Assert.Equal(4, result.PageCount);
        }

        [Fact]
        public void PaginatedResult_PageCount_ZeroPageSize_ReturnsZero()
        {
            var result = new PaginatedResult<int>(new List<int>(), 50, 1, 0);
            Assert.Equal(0, result.PageCount);
        }

        [Fact]
        public void PaginatedResult_PageCount_ComputedWithCeiling()
        {
            var result = new PaginatedResult<int>(new List<int>(), 10, 1, 3);
            Assert.Equal(4, result.PageCount);

            var exact = new PaginatedResult<int>(new List<int>(), 10, 1, 5);
            Assert.Equal(2, exact.PageCount);
        }

        #endregion

        #region GroupResult

        [Fact]
        public void GroupResult_DefaultConstructor_InitializesItems()
        {
            var result = new GroupResult<string, SampleEntity>();
            Assert.NotNull(result.Items);
            Assert.Empty(result.Items);
            Assert.Null(result.Key);
            Assert.Null(result.Aggregates);
        }

        [Fact]
        public void GroupResult_ParameterizedConstructor_SetsValues()
        {
            var items = new List<SampleEntity> { new SampleEntity { DocNumber = 1 } };
            var aggregates = new Dictionary<decimal, DataAggregationType> { { 1m, DataAggregationType.Sum } };
            var result = new GroupResult<string, SampleEntity>("Group A", items, aggregates);

            Assert.Equal("Group A", result.Key);
            Assert.Same(items, result.Items);
            Assert.Same(aggregates, result.Aggregates);
        }

        [Fact]
        public void GroupResult_Setters_Assignable()
        {
            var result = new GroupResult<int, SampleEntity>();
            var aggregates = new Dictionary<decimal, DataAggregationType> { { 5m, DataAggregationType.Count } };
            result.Key = 7;
            result.Aggregates = aggregates;
            result.Items = new List<SampleEntity> { new SampleEntity { DocNumber = 2 } };

            Assert.Equal(7, result.Key);
            Assert.Equal(DataAggregationType.Count, result.Aggregates[5m]);
            Assert.Single(result.Items);
        }

        #endregion

        #region RelatedEntityAttribute

        [Fact]
        public void RelatedEntityAttribute_GetMethods_ReturnSetValues()
        {
            var attribute = new RelatedEntityAttribute
            {
                Cardinality = RelationCardinality.ManyToMany,
                ForeignKeyAttribute = "RightSideId",
                IntermediaryEntity = typeof(SampleIntermedyForeignEntity),
                IntermediaryKeyAttribute = "LeftSideId"
            };

            Assert.Equal(RelationCardinality.ManyToMany, attribute.GetRelationCardinality());
            Assert.Equal(typeof(SampleIntermedyForeignEntity), attribute.GetIntermediaryEntity());
            Assert.Equal("LeftSideId", attribute.GetIntermediaryKeyAttribute());
        }

        [Fact]
        public void RelatedEntityAttribute_DefaultValues()
        {
            var attribute = new RelatedEntityAttribute();
            Assert.Equal(0, (int)attribute.GetRelationCardinality());
            Assert.Null(attribute.GetIntermediaryEntity());
            Assert.Null(attribute.GetIntermediaryKeyAttribute());
        }

        #endregion

        #region RelationalColumn

        [Fact]
        public void RelationalColumn_GetColumnName_ReturnsSetValue()
        {
            var column = new RelationalColumn
            {
                TableName = "dim_product",
                IntermediaryColumnName = "inter",
                ColumnName = "product_name",
                ColumnAlias = "ProductName",
                KeyColumn = "product_id",
                ForeignKeyColumn = "id",
                IntermediaryColumnKey = "inter_key",
                JunctionType = RelationalJunctionType.Mandatory,
                Filterable = true
            };

            Assert.Equal("product_name", column.GetColumnName());
            Assert.Equal("dim_product", column.TableName);
            Assert.Equal("ProductName", column.ColumnAlias);
            Assert.Equal(RelationalJunctionType.Mandatory, column.JunctionType);
            Assert.True(column.Filterable);
        }

        #endregion

        #region RangeFilterAttribute

        [Fact]
        public void RangeFilterAttribute_LinkedRangeProperty()
        {
            var attribute = new RangeFilterAttribute { LinkedRangeProperty = "AgeEnd" };
            Assert.Equal("AgeEnd", attribute.LinkedRangeProperty);
        }

        #endregion
    }
}