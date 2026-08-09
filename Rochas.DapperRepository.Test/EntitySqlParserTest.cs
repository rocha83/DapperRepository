using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Helpers;

namespace Rochas.DapperRepository.Test
{
    public class EntitySqlParserTest
    {
        #region Original Tests

        [Fact]
        public void GetByPrimaryKeyTest()
        {
            var entityType = typeof(SampleEntity);
            var entityProps = entityType.GetProperties();
            var testFilter = EntityReflector.GetFilterByPrimaryKey(entityType, entityProps, 12345);

            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.Get, testFilter);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "sample_entity", "id"), result);
        }

        [Fact]
        public void GetByFilterTest()
        {
            var testFilter = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.Get, testFilter);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "sample_entity", "doc_number"), result);
        }

        [Fact]
        public void ListTest()
        {
            var testFilter = new SampleEntity() { Name = "roberto" };
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.Query, testFilter);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("FROM", result);
            Assert.Contains("LIKE", result);
            Assert.Contains("roberto", result);
        }

        [Fact]
        public void ListLimitedTest()
        {
            var testFilter = new SampleEntity() { Name = "roberto" };
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.Query, testFilter, 5);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("FROM", result);
            Assert.Contains("LIKE", result);
            Assert.Contains("roberto", result);
            Assert.EndsWith("LIMIT 5", result);
        }

        [Fact]
        public void ListSortedTest()
        {
            var testFilter = new SampleEntity() { };
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.Query, testFilter, sortAttributes: "Name");
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("FROM", result);
            Assert.EndsWith("ORDER BY name ASC", result);
        }

        [Fact]
        public void ListLimitedSQLServerTest()
        {
            var testFilter = new SampleEntity() { Name = "roberto" };
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLServer, PersistenceAction.Query, testFilter, 5);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("TOP 5", result);
            Assert.Contains("FROM", result);
            Assert.Contains("LIKE", result);
            Assert.Contains("roberto", result);
        }

        [Fact]
        public void SearchTest()
        {
            var filterType = typeof(SampleEntity);
            var filterProps = filterType.GetProperties();
            var testFilter = EntityReflector.GetFilterByFilterableColumns(typeof(SampleEntity), filterProps, "roberto");

            var sqlParameters = new Dictionary<string, object>();
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.Query, testFilter, sqlParameters: sqlParameters);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("FROM", result);
            Assert.Contains("@p0", result);
            Assert.Contains("@p1", result);
            Assert.Contains("LIKE", result);
            Assert.Contains("OR", result);
            Assert.Equal("%roberto%", sqlParameters["@p0"]);
            Assert.Equal("%roberto%", sqlParameters["@p1"]);
        }

        [Fact]
        public void SearchParameterized_ReturnsCorrectParameters()
        {
            var filterType = typeof(SampleEntity);
            var filterProps = filterType.GetProperties();
            var testFilter = EntityReflector.GetFilterByFilterableColumns(typeof(SampleEntity), filterProps, "busca teste");

            var sqlParameters = new Dictionary<string, object>();
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.Query, testFilter, sqlParameters: sqlParameters);
            result = result.Trim();

            Assert.DoesNotContain("'%busca teste%'", result);
            Assert.Contains("@p0", result);
            Assert.Contains("@p1", result);
            Assert.Equal("%busca teste%", sqlParameters["@p0"]);
            Assert.Equal("%busca teste%", sqlParameters["@p1"]);
        }

        [Fact]
        public void CreateTest()
        {
            var sampleEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(sampleEntity, DatabaseEngine.SQLite, PersistenceAction.Add);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("INSERT INTO", result);
            Assert.Contains("VALUES", result);
            Assert.Contains("creation_date", result);
            Assert.Contains("name", result);
            Assert.Contains("active", result);
        }

        [Fact]
        public void EditTest()
        {
            var editedEntity = new SampleEntity() { Name = "roberto gomes", Age = 35 };
            var filterEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(editedEntity, DatabaseEngine.SQLite, PersistenceAction.Update, filterEntity);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("UPDATE", result);
            Assert.Contains("SET", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "sample_entity", "doc_number"), result);
        }

        [Fact]
        public void DeleteTest()
        {
            var filterEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(filterEntity, DatabaseEngine.SQLite, PersistenceAction.Remove, filterEntity);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("DELETE FROM", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "sample_entity", "doc_number"), result);
        }

        [Fact]
        public void CountTest()
        {
            var filterEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(filterEntity, DatabaseEngine.SQLite, PersistenceAction.Count, filterEntity);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT COUNT", result);
            Assert.Contains("FROM", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "sample_entity", "doc_number"), result);
        }

        [Fact]
        public void EntityWithoutTableAttribute_ShouldFallbackToClassName()
        {
            var entityType = typeof(SampleNoTableEntity);
            var entityProps = entityType.GetProperties()
                .Where(p => !p.PropertyType.Namespace.StartsWith("Rochas")).ToArray();
            var testFilter = EntityReflector.GetFilterByPrimaryKey(entityType, entityProps, 1);

            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.Get, testFilter);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("SampleNoTableEntity", result);
        }

        [Fact]
        public void DebugQueryWithParameters()
        {
            var filter = new SampleEntity() { Name = "roberto" };
            var sqlParameters = new Dictionary<string, object>();
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter, sqlParameters: sqlParameters);
            result = result.Trim();

            Console.WriteLine($"SQL: {result}");
            Console.WriteLine($"Parameters: {string.Join(", ", sqlParameters.Select(p => $"{p.Key}={p.Value}"))}");

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
        }

        #endregion

        #region Date Range - value type detection (Fix 1)

        [Fact]
        public void DateRange_BothBounds_ShouldUseBetween()
        {
            var filter = new SampleEntity()
            {
                CreationDate = DateTime.Now.Date.AddDays(-1),
                CreationDateEnd = DateTime.Now.Date.AddDays(1)
            };

            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.Contains("BETWEEN", result);
            Assert.Contains("creation_date", result);
        }

        [Fact]
        public void DateRange_OnlyFrom_ShouldUseGreaterOrEqual()
        {
            var filter = new SampleEntity()
            {
                CreationDate = DateTime.Now.Date.AddDays(-7)
            };

            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.Contains(">=", result);
            Assert.Contains("creation_date", result);
            Assert.DoesNotContain("BETWEEN", result);
        }

        [Fact]
        public void DateRange_OnlyTo_ShouldUseLessOrEqual()
        {
            var filter = new SampleEntity()
            {
                CreationDateEnd = DateTime.Now.Date.AddDays(1)
            };

            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.Contains("<=", result);
            Assert.Contains("creation_date", result);
            Assert.DoesNotContain("BETWEEN", result);
        }

        [Fact]
        public void DateRange_NonDateColumn_ShouldNotDetectAsRange()
        {
            var filter = new SampleEntity()
            {
                Name = "test"
            };

            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.Contains("LIKE", result);
            Assert.DoesNotContain("BETWEEN", result);
        }

        #endregion

        #region Empty String Exclusion (Fix 3)

        [Fact]
        public void EmptyString_ShouldBeExcludedFromWhere()
        {
            var filter = new SampleEntity() { Name = "" };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.DoesNotContain("LIKE", result);
            Assert.DoesNotContain("WHERE sample_entity.name", result);
        }

        [Fact]
        public void NonEmptyString_ShouldUseLikeInQuery()
        {
            var filter = new SampleEntity() { Name = "roberto" };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.Contains("LIKE", result);
            Assert.Contains("WHERE sample_entity.name", result);
        }

        [Fact]
        public void NullString_ShouldBeExcludedFromWhere()
        {
            var filter = new SampleEntity() { Name = null };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.DoesNotContain("LIKE", result);
            Assert.DoesNotContain("WHERE sample_entity.name", result);
            Assert.Contains("WHERE 1 = 1", result);
        }

        [Fact]
        public void MixedFilters_WithEmptyString_ShouldOnlyFilterNonEmpty()
        {
            var filter = new SampleEntity() { Name = "", DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.DoesNotContain("WHERE sample_entity.name", result);
            Assert.Contains("WHERE sample_entity.doc_number", result);
        }

        #endregion

        #region Array Property Skip (Fix 5)

        [Fact]
        public void ArrayProperty_ShouldNotAppearInQuery()
        {
            var filter = new SampleArrayEntity() { Name = "test" };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.Contains("name", result);
            Assert.DoesNotContain("hash_codes", result);
        }

        [Fact]
        public void ArrayProperty_ShouldNotAppearInInsert()
        {
            var entity = new SampleArrayEntity() { Name = "test", Active = true };
            var result = EntitySqlParser.ParseEntity(entity, DatabaseEngine.SQLite, PersistenceAction.Add);
            result = result.Trim();

            Assert.StartsWith("INSERT INTO", result);
            Assert.Contains("name", result);
            Assert.DoesNotContain("hash_codes", result);
        }

        [Fact]
        public void ArrayProperty_ShouldNotAppearInUpdate()
        {
            var entity = new SampleArrayEntity() { Name = "updated" };
            var filter = new SampleArrayEntity() { Id = 1 };
            var result = EntitySqlParser.ParseEntity(entity, DatabaseEngine.SQLite, PersistenceAction.Update, filter);
            result = result.Trim();

            Assert.StartsWith("UPDATE", result);
            Assert.Contains("name", result);
            Assert.DoesNotContain("hash_codes", result);
        }

        #endregion

        #region Get=Equality vs Query=Like

        [Fact]
        public void Get_StringFilter_ShouldUseEquality()
        {
            var filter = new SampleEntity() { Name = "roberto" };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Get, filter);
            result = result.Trim();

            Assert.DoesNotContain("LIKE", result);
            Assert.Contains("=", result);
        }

        [Fact]
        public void Count_StringFilter_ShouldUseLike()
        {
            var filter = new SampleEntity() { Name = "roberto" };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Count, filter);
            result = result.Trim();

            Assert.Contains("LIKE", result);
        }

        [Fact]
        public void Get_NumericFilter_ShouldUseEquality()
        {
            var filter = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Get, filter);
            result = result.Trim();

            Assert.DoesNotContain("LIKE", result);
            Assert.Contains("=", result);
        }

        #endregion

        #region filterConjunction AND vs OR

        [Fact]
        public void Query_ConjunctionTrue_ShouldUseAnd()
        {
            var filter = new SampleEntity() { Name = "roberto", Active = true };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter, filterConjunction: true);
            result = result.Trim();

            Assert.Contains("AND", result);
        }

        [Fact]
        public void Query_ConjunctionFalse_ShouldUseOr()
        {
            var filter = new SampleEntity() { Name = "roberto", Resume = "brasil" };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter, filterConjunction: false);
            result = result.Trim();

            Assert.Contains("OR", result);
        }

        #endregion

        #region Numeric Filter Behavior

        [Fact]
        public void Query_NumericFilter_ShouldUseEquality()
        {
            var filter = new SampleEntity() { Age = 30 };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.Contains("age", result);
            Assert.DoesNotContain("LIKE", result);
        }

        [Fact]
        public void Query_NumericRange_ShouldUseBetween()
        {
            var filter = new SampleEntity()
            {
                Age = 18,
                AgeEnd = 65
            };

            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.Contains("BETWEEN", result);
            Assert.Contains("age", result);
        }

        #endregion

        #region Boolean Filter

        [Fact]
        public void Query_BoolTrue_ShouldUseEqual1()
        {
            var filter = new SampleEntity() { Active = true };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.Contains("= 1", result);
        }

        [Fact]
        public void Get_BoolTrue_ShouldUseEqual1()
        {
            var filter = new SampleEntity() { Active = true };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Get, filter);
            result = result.Trim();

            Assert.Contains("= 1", result);
        }

        #endregion

        #region Null Values Excluded

        [Fact]
        public void Query_MultipleProperties_OnlyNonNull_ShouldFilter()
        {
            var filter = new SampleEntity() { Name = "roberto", Resume = null };
            var result = EntitySqlParser.ParseEntity(filter, DatabaseEngine.SQLite, PersistenceAction.Query, filter);
            result = result.Trim();

            Assert.Contains("WHERE sample_entity.name", result);
            Assert.DoesNotContain("WHERE sample_entity.resume", result);
        }

        #endregion
    }
}
