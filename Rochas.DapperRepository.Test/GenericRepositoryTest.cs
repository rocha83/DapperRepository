using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Specification.Models;
using Rochas.Extensions;

namespace Rochas.DapperRepository.Test
{
    [TestCaseOrderer("Rochas.DapperRepository.Test.AlphabeticalOrderer", "Rochas.DapperRepository.Test")]
    public class GenericRepositoryTest
    {
        private string databaseFileName = "MockDatabase.sqlite";
        private string connString = "Data Source=MockDatabase.sqlite;Version=3;New=True;";

        #region Mock Repository Initialization

        [Fact]
        public void Test01_Initialize()
        {
            var tableScript = @"CREATE TABLE [sample_entity](
                                             [id] INTEGER PRIMARY KEY,
                                             [child_id] [int] NULL,
                                             [doc_number] [int] NOT NULL,
	                                         [creation_date] [datetime] NOT NULL,
	                                         [name] [varchar](200) NOT NULL,
                                             [resume] [varchar](800) NULL,
	                                         [age] [int] NULL,
	                                         [height] [decimal](18, 2) NULL,
	                                         [weight] [decimal](18, 2) NULL,
	                                         [active] [bit] NOT NULL)";

            var oneForeignTableScript = @"CREATE TABLE [sample_one_foreign_entity] (
	                                                   [parent_id] [int] PRIMARY KEY NOT NULL,
                                                       [title] [varchar](100) NOT NULL,
	                                                   [description] [varchar](400) NOT NULL)";

            var manyForeignTableScript = @"CREATE TABLE [sample_many_foreign_entity] (
	                                                    [id] INTEGER PRIMARY KEY,
                                                        [parent_id] [int] NULL,
                                                        [creation_date] [datetime] NOT NULL,
                                                        [code] [int] NULL,
                                                        [title] [varchar](100) NOT NULL,
	                                                    [description] [varchar](400) NULL,
                                                        [active] [bit] NOT NULL)";

            var intermedyForeignTableScript = @"CREATE TABLE [sample_intermedy_foreign_entity] (
                                                             [id] INTEGER PRIMARY KEY,
                                                             [left_side_id] [int] NOT NULL,
                                                             [right_side_id] [int] NOT NULL,
                                                             [active] [bit] NOT NULL)";

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(tableScript, databaseFileName);
                repos.Initialize(oneForeignTableScript);
                repos.Initialize(manyForeignTableScript);
                repos.Initialize(intermedyForeignTableScript);
            }
        }

        #endregion

        #region Single Entity Tests

        [Fact]
        public void Test02_Add()
        {
            int result;
            var sampleEntity1 = new SampleEntity()
            {
                DocNumber = 12345,
                CreationDate = DateTime.Now,
                Name = "Roberto Torres",
                Resume = "Technology Professional from Sao Paulo Brazil",
                Age = 32,
                Active = true
            };

            var sampleEntity2 = new SampleEntity()
            {
                DocNumber = 76910,
                CreationDate = DateTime.Now,
                Name = "Gustavo Meireles",
                Resume = "Technology Professional from Rio de Janeiro Brazil",
                Age = 25,
                Active = true
            };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.AddSync(sampleEntity1);
            }
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result += repos.AddSync(sampleEntity2);
            }

            Assert.True(result > 1);
        }

        [Fact]
        public void Test03_AddComposition()
        {
            int result;
            var sampleEntity = new SampleEntity()
            {
                DocNumber = 12345,
                CreationDate = DateTime.Now,
                Name = "Roberto Torres",
                Resume = "Technology Professional from Sao Paulo Brazil",
                Age = 32,
                Active = true,
                OneToManyForeignEntities = new List<SampleManyForeignEntity>() {
                    new SampleManyForeignEntity()
                    {
                        Code = 444666,
                        Title = "New Many Entity Composition Test",
                        CreationDate = DateTime.Now,
                        Active = true
                    }
                }
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.AddSync(sampleEntity, true);
            }

            Assert.True(result > 1);
        }

        [Fact]
        public void Test04_GetByKey()
        {
            SampleEntity result;

            var key = 1;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.GetSync(key);
            }

            Assert.NotNull(result);
            Assert.Equal(key, result.Id);
        }

        [Fact]
        public void Test05_GetByFilter()
        {
            SampleEntity result;

            var filter = new SampleEntity() { DocNumber = 12345 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.GetSync(filter);
            }

            Assert.NotNull(result);
            Assert.Equal(filter.DocNumber, result.DocNumber);
        }

        [Fact]
        public void Test06_Query()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "roberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test07_QueryLimited()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "roberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter, recordsLimit: 5);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
            Assert.True(result.Count <= 5);
        }

        [Fact]
        public void Test08_QuerySorted()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter,
                                         sortAttributes: "Name");
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
            Assert.StartsWith("Gustavo", result.First().Name);

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter,
                                         sortAttributes: "Name",
                                         orderDescending: true);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
            Assert.StartsWith("Roberto", result.First().Name);
        }

        [Fact]
        public void Test09_QueryByDateRange()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity()
            {
                CreationDate = DateTime.Now.Date.AddDays(-1),
                CreationDateEnd = DateTime.Now.Date.AddDays(1)
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter, filterConjunction: false);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test10_QueryByAgeMajorThan()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity()
            {
                Age = 16
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test11_Search()
        {
            ICollection<SampleEntity> result;

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var criteria = "torres";

                result = repos.SearchSync(criteria);
                Assert.NotNull(result);
                Assert.True(result.Any());

                criteria = "sao paulo";

                result = repos.SearchSync(criteria);
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test12_BulkSearch()
        {
            ICollection<SampleEntity> result;

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var criterias = "roberto torres sao pedro".Tokenize();

                result = repos.BulkSearchSync(criterias);
                
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test13_Count()
        {
            int result = 0;
            var filter = new SampleEntity() { Name = "roberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.CountSync(filter);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test14_Update()
        {
            int result = 0;
            var filter = new SampleEntity() { DocNumber = 12345 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var item = repos.GetSync(filter);
                if (item != null)
                {
                    item.Age = 37;
                    result = repos.UpdateSync(item, filter);
                }
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test15_UpdateComposition()
        {
            int result = 0;
            var filter = new SampleEntity() { DocNumber = 12345 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var item = repos.GetSync(filter);
                if (item != null)
                {
                    item.Age = 37;
                    item.OneToManyForeignEntities = new List<SampleManyForeignEntity>() {
                        new SampleManyForeignEntity()
                        {
                            Code = 555777,
                            Title = "New Many Entity Composition Test",
                            CreationDate = DateTime.Now,
                            Active = true
                        }
                    };
                    result = repos.UpdateSync(item, filter, true);
                }
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test16_Remove()
        {
            int result = 0;
            var filter = new SampleEntity() { DocNumber = 12345 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.RemoveSync(filter);
            }

            filter = new SampleEntity() { DocNumber = 76910 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result += repos.RemoveSync(filter);
            }

            Assert.True(result > 1);
        }

        #endregion

        #region Pagination Tests

        [Fact]
        public void Test40_SearchPaginated()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(new SampleEntity { DocNumber = 90001, CreationDate = DateTime.Now, Name = "Paginated Alpha", Resume = "Test pagination", Age = 20, Active = true });
                repos.AddSync(new SampleEntity { DocNumber = 90002, CreationDate = DateTime.Now, Name = "Paginated Beta", Resume = "Test pagination", Age = 25, Active = true });
                repos.AddSync(new SampleEntity { DocNumber = 90003, CreationDate = DateTime.Now, Name = "Paginated Gamma", Resume = "Test pagination", Age = 30, Active = true });
            }

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchPaginatedSync("paginated", page: 1, pageSize: 2);

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.Equal(1, result.Page);
                Assert.Equal(2, result.PageSize);
                Assert.True(result.TotalCount >= 3);
                Assert.True(result.PageCount >= 2);
            }
        }

        [Fact]
        public void Test41_QueryPaginated()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryPaginatedSync(filter, page: 1, pageSize: 5);

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.Equal(1, result.Page);
                Assert.Equal(5, result.PageSize);
                Assert.True(result.TotalCount > 0);
            }
        }

        [Fact]
        public async Task Test42_SearchPaginated_Async()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = await repos.SearchPaginated("paginated", page: 1, pageSize: 10);

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.True(result.TotalCount >= 1);
            }
        }

        [Fact]
        public async Task Test43_QueryPaginated_Async()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = await repos.QueryPaginated(filter, page: 1, pageSize: 5);

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.True(result.TotalCount > 0);
            }
        }

        [Fact]
        public void Test44_SearchPaginated_WithSort()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchPaginatedSync("paginated", page: 1, pageSize: 10, sortAttributes: "Name", orderDescending: true);

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.StartsWith("Paginated G", result.Items.First().Name);
            }
        }

        [Fact]
        public void Test45_QueryPaginated_WithSort()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryPaginatedSync(filter, page: 1, pageSize: 10, sortAttributes: "Name");

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
            }
        }

        #endregion

        #region OneToOne Composite Entity Tests

        [Fact]
        public void Test24_OneToOneCompositionAdd()
        {
            int result;
            var sampleEntity = new SampleEntity()
            {
                DocNumber = 13456,
                CreationDate = DateTime.Now,
                Name = "Alberto Gomes",
                Active = true,
                OneToOneForeignEntity = new SampleOneForeignEntity()
                {
                    Title = "Titulo Teste Singular",
                    Description = "Descricao Teste Lorem Ipsum Lorem Ipsum"
                }
            };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.AddSync(sampleEntity, true);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test25_GetOneToOneCompositionByKey()
        {
            SampleEntity result;

            var key = 1;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.GetSync(key, true);
            }

            Assert.NotNull(result);
            Assert.Equal(key, result.Id);

            Assert.NotNull(result.OneToOneForeignEntity);
        }

        [Fact]
        public void Test26_QueryOneToOneComposition()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "alberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter, true);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());

            Assert.NotNull(result.First().OneToOneForeignEntity);
        }

        #endregion

        #region ManyToOne Composition Entity Tests

        [Fact]
        public void Test27_ManyToOneCompositionAdd()
        {
            int result;

            var manyToOneForeignEntity = new SampleManyForeignEntity()
            {
                CreationDate = DateTime.Now,
                Title = "Titulo Teste Estrangeira Singular",
                Description = "Descricao Estrangeira Teste Lorem Ipsum Lorem Ipsum",
                Active = true
            };

            using (var repos = new GenericRepository<SampleManyForeignEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.AddSync(manyToOneForeignEntity, true);
            }

            var sampleEntity = new SampleEntity()
            {
                ChildId = result,
                DocNumber = 14567,
                CreationDate = DateTime.Now,
                Name = "Claudia Oliveira",
                Active = true
            };

            result = 0;

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.AddSync(sampleEntity, true);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test28_GetManyToOneCompositionByKey()
        {
            SampleEntity result;

            var key = 1;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.GetSync(key, true);
            }

            Assert.NotNull(result);
            Assert.Equal(key, result.Id);

            Assert.NotNull(result.ManyToOneForeignEntity);
        }

        [Fact]
        public void Test29_QueryManyToOneComposition()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "claudia" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter, true);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());

            Assert.NotNull(result.First().ManyToOneForeignEntity);
        }

        #endregion

        #region OneToMany Composite Entity Tests

        [Fact]
        public void Test30_OneToManyCompositionAdd()
        {
            int result;
            var sampleEntity = new SampleEntity()
            {
                DocNumber = 13456,
                CreationDate = DateTime.Now,
                Name = "Carlos Almeida",
                Active = true,
                OneToManyForeignEntities = new List<SampleManyForeignEntity>()
            };

            for (var counter = 1; counter < 6; counter++)
                sampleEntity.OneToManyForeignEntities.Add(new SampleManyForeignEntity()
                {
                    CreationDate = sampleEntity.CreationDate,
                    Title = $"Titulo Teste Plural {counter}",
                    Description = $"Descricao Item {counter} Lorem Ipsum Lorem Ipsum",
                    Active = true
                });

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.AddSync(sampleEntity, true);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test31_GetOneToManyCompositionByKey()
        {
            SampleEntity result;

            var key = 3;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.GetSync(key, true);
            }

            Assert.NotNull(result);
            Assert.Equal(key, result.Id);

            Assert.NotNull(result.OneToManyForeignEntities);
            Assert.True(result.OneToManyForeignEntities.Count == 6);
        }

        [Fact]
        public void Test32_QueryOneToManyComposition()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "carlos" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter, true);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());

            var firstItem = result.First();
            Assert.NotNull(firstItem.OneToManyForeignEntities);
            Assert.True(firstItem.OneToManyForeignEntities.Count == 6);
        }

        #endregion

        #region ManyToMany Composite Entity Tests

        [Fact]
        public void Test33_IntermedyCompositionCreate()
        {
            int leftEntityResult;
            var sampleLeftEntity = new SampleEntity()
            {
                DocNumber = 15678,
                CreationDate = DateTime.Now,
                Name = "Danilo Almeida",
                Active = true
            };

            int rightEntityResult;
            var sampleRightEntity = new SampleManyForeignEntity()
            {
                Code = 123,
                CreationDate = DateTime.Now,
                Title = "Ajudante Geral",
                Active = true
            };

            using var leftEntityRepos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString);
            leftEntityResult = leftEntityRepos.AddSync(sampleLeftEntity);

            using var rightEntityRepos = new GenericRepository<SampleManyForeignEntity>(DatabaseEngine.SQLite, connString);
            rightEntityResult = rightEntityRepos.AddSync(sampleRightEntity);

            var sampleIntermedyEntity = new SampleIntermedyForeignEntity()
            {
                LeftSideId = leftEntityResult,
                RightSideId = rightEntityResult
            };

            using var repos = new GenericRepository<SampleIntermedyForeignEntity>(DatabaseEngine.SQLite, connString);
            var result = repos.AddSync(sampleIntermedyEntity);

            Assert.True(result > 0);
        }

        [Fact]
        public void Test34_IntermedyCompositionGet()
        {
            using var leftEntityRepos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString);
            var leftEntityFilter = new SampleEntity() { DocNumber = 15678 };
            var leftEntityResult = leftEntityRepos.GetSync(leftEntityFilter, true);

            using var rightEntityRepos = new GenericRepository<SampleManyForeignEntity>(DatabaseEngine.SQLite, connString);
            var rightEntityFilter = new SampleManyForeignEntity() { Code = 123 };
            var rightEntityResult = rightEntityRepos.GetSync(rightEntityFilter, true);

            Assert.NotNull(leftEntityResult);
            Assert.NotNull(rightEntityResult);

            Assert.True(leftEntityResult.ManyToManyForeignEntities.Count > 0);
            Assert.Equal(123, leftEntityResult.ManyToManyForeignEntities.Single().Code);

            Assert.True(rightEntityResult.ManyToManyForeignEntities.Count > 0);
            Assert.Equal(15678, rightEntityResult.ManyToManyForeignEntities.Single().DocNumber);
        }

        #endregion

        #region AddRange and BulkSqlCreateRange Tests

        [Fact]
        public void Test35_AddRange()
        {
            var entities = new List<SampleManyForeignEntity>
            {
                new SampleManyForeignEntity { Code = 1001, Title = "Bulk 1", CreationDate = DateTime.Now, Active = true },
                new SampleManyForeignEntity { Code = 1002, Title = "Bulk 2", CreationDate = DateTime.Now, Active = true },
                new SampleManyForeignEntity { Code = 1003, Title = "Bulk 3", CreationDate = DateTime.Now, Active = true }
            };

            using (var repos = new GenericRepository<SampleManyForeignEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddRangeSync(entities);
            }

            var count = 0;
            using (var repos = new GenericRepository<SampleManyForeignEntity>(DatabaseEngine.SQLite, connString))
            {
                count = repos.CountSync(new SampleManyForeignEntity());
            }

            Assert.True(count >= 3);
        }

        #endregion

        #region Edge Cases

        [Fact]
        public void Test36_GetByKey_EmptyKey_ReturnsNull()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.GetSync(0);
                Assert.Null(result);
            }
        }

        [Fact]
        public void Test37_GetByKey_NonExistentKey_ReturnsNull()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.GetSync(99999);
                Assert.Null(result);
            }
        }

        [Fact]
        public void Test38_SearchPaginated_PageSizeOne()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchPaginatedSync("almeida", page: 1, pageSize: 1);
                Assert.NotNull(result);
                Assert.True(result.Items.Count <= 1);
                Assert.True(result.TotalCount >= 1);
                Assert.True(result.PageCount >= 1);
            }
        }

        [Fact]
        public void Test39_QueryPaginated_EmptyFilter()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QueryPaginatedSync(new SampleEntity(), page: 1, pageSize: 10);
                Assert.NotNull(result);
                Assert.True(result.Items.Any());
            }
        }

        #endregion

        #region v1.6.8 Fixes - End-to-End Tests

        [Fact]
        public void Test46_EmptyStringFilter_ShouldNotAffectQuery()
        {
            var filter = new SampleEntity() { Name = "" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test49_NullStringFilter_ShouldNotAffectQuery()
        {
            var filter = new SampleEntity() { Name = null, Resume = null };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test50_Count_EmptyStringFilter()
        {
            var filter = new SampleEntity() { Name = "" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var count = repos.CountSync(filter);
                Assert.True(count > 0);
            }
        }

        [Fact]
        public void Test51_MixedFilters_EmptyAndNonEmpty()
        {
            var filter = new SampleEntity() { Name = "", Active = true };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());
                Assert.All(result, e => Assert.True(e.Active));
            }
        }

        [Fact]
        public void Test52_DateRange_EndToEnd()
        {
            var filter = new SampleEntity()
            {
                CreationDate = DateTime.Now.Date.AddDays(-30),
                CreationDateEnd = DateTime.Now.Date.AddDays(1)
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter, filterConjunction: false);
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test53_ArrayProperty_Insert_SkipsArray()
        {
            var tableScript = @"CREATE TABLE IF NOT EXISTS [sample_array_entity] (
                                        [id] INTEGER PRIMARY KEY,
                                        [name] [varchar](200) NULL,
                                        [hash_codes] [blob] NULL,
                                        [active] [bit] NOT NULL)";

            using (var repos = new GenericRepository<SampleArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(tableScript);
            }

            var entity = new SampleArrayEntity()
            {
                Name = "array test",
                HashCodes = new uint[] { 12345, 67890 },
                Active = true
            };

            int id;
            using (var repos = new GenericRepository<SampleArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                id = repos.AddSync(entity);
            }

            Assert.True(id > 0);
        }

        [Fact]
        public void Test54_ArrayProperty_Query_SkipsArray()
        {
            var entity = new SampleArrayEntity()
            {
                Name = "query array test",
                HashCodes = new uint[] { 11111, 22222 },
                Active = true
            };

            using (var repos = new GenericRepository<SampleArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            var filter = new SampleArrayEntity() { Name = "query array test" };

            using (var repos = new GenericRepository<SampleArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());
                Assert.Equal("query array test", result.First().Name);
            }
        }

        #endregion

        #region QueryBuilder Tests

        [Fact]
        public void Test55_QueryBuilder_FilterOnly()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryBuilder(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test56_QueryBuilder_FilterOnly_Await()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryBuilder(filter).GetAwaiter().GetResult();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test57_QueryBuilder_OrderBy_SingleColumn()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryBuilder(filter).OrderBy("Name").ToList();
                Assert.NotNull(result);
                Assert.True(result.Count > 1);
                var list = result.ToList();
                Assert.True(list[0].Name.CompareTo(list[1].Name) <= 0);
            }
        }

        [Fact]
        public void Test58_QueryBuilder_OrderBy_Descending()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryBuilder(filter).OrderBy("Name").Descending().ToList();
                Assert.NotNull(result);
                Assert.True(result.Count > 1);
                var list = result.ToList();
                Assert.True(list[0].Name.CompareTo(list[1].Name) >= 0);
            }
        }

        [Fact]
        public void Test59_QueryBuilder_OrderBy_MultipleColumns()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryBuilder(filter).OrderBy(new[] { "Age", "Name" }).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test60_QueryBuilder_OrderBy_Await()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryBuilder(filter).OrderBy("Name").GetAwaiter().GetResult();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test61_QueryBuilder_OrderBy_Descending_Await()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryBuilder(filter).OrderBy("Name", descending: true).GetAwaiter().GetResult();
                Assert.NotNull(result);
                Assert.True(result.Count > 1);
                var list = result.ToList();
                Assert.True(list[0].Name.CompareTo(list[1].Name) >= 0);
            }
        }

        [Fact]
        public void Test62_QueryBuilder_EmptyFilter()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity();
                var result = repos.QueryBuilder(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test63_QueryBuilder_FilterConjunction()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true, Age = 32 };
                var result = repos.QueryBuilder(filter, filterConjunction: true).OrderBy("Name").ToList();
                Assert.NotNull(result);
            }
        }

        #endregion

        #region QueryPaginatedBuilder Tests

        [Fact]
        public void Test64_QueryPaginatedBuilder_Paginate()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryPaginatedBuilder(filter).Paginate(1, 2);
                Assert.NotNull(result);
                Assert.True(result.Items.Count <= 2);
                Assert.True(result.TotalCount > 0);
                Assert.Equal(1, result.Page);
                Assert.Equal(2, result.PageSize);
            }
        }

        [Fact]
        public void Test65_QueryPaginatedBuilder_Paginate_Page2()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryPaginatedBuilder(filter).Paginate(2, 1);
                Assert.NotNull(result);
                Assert.True(result.Items.Count <= 1);
                Assert.True(result.TotalCount > 0);
                Assert.Equal(2, result.Page);
            }
        }

        [Fact]
        public void Test66_QueryPaginatedBuilder_OrderBy()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryPaginatedBuilder(filter).OrderBy("Name").Paginate(1, 10);
                Assert.NotNull(result);
                Assert.True(result.Items.Count > 1);
                Assert.True(result.Items.ElementAt(0).Name.CompareTo(result.Items.ElementAt(1).Name) <= 0);
            }
        }

        [Fact]
        public void Test67_QueryPaginatedBuilder_OrderBy_Descending()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryPaginatedBuilder(filter).OrderBy("Name").Descending().Paginate(1, 10);
                Assert.NotNull(result);
                Assert.True(result.Items.Count > 1);
                Assert.True(result.Items[0].Name.CompareTo(result.Items[1].Name) >= 0);
            }
        }

        [Fact]
        public void Test68_QueryPaginatedBuilder_OrderBy_Await()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QueryPaginatedBuilder(filter).OrderBy("Name").PaginateAsync(1, 5).GetAwaiter().GetResult();
                Assert.NotNull(result);
                Assert.True(result.Items.Any());
            }
        }

        [Fact]
        public void Test69_QueryPaginatedBuilder_EmptyFilter()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity();
                var result = repos.QueryPaginatedBuilder(filter).Paginate(1, 5);
                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.True(result.TotalCount > 0);
            }
        }

        #endregion

        #region RelationalColumn Tests

        [Fact]
        public void Test70_RelationalColumn_CreateTables()
        {
            var dimProductScript = @"CREATE TABLE IF NOT EXISTS [dim_product] (
                                        [id] INTEGER PRIMARY KEY,
                                        [product_name] [varchar](200) NOT NULL,
                                        [category] [varchar](100) NULL,
                                        [price] [decimal](18, 2) NULL)";

            var dimCustomerScript = @"CREATE TABLE IF NOT EXISTS [dim_customer] (
                                        [id] INTEGER PRIMARY KEY,
                                        [customer_name] [varchar](200) NOT NULL,
                                        [region] [varchar](100) NULL)";

            var factSalesScript = @"CREATE TABLE IF NOT EXISTS [fact_sales] (
                                        [id] INTEGER PRIMARY KEY,
                                        [sale_date] [varchar](20) NULL,
                                        [product_id] [int] NOT NULL,
                                        [customer_id] [int] NOT NULL,
                                        [quantity] [int] NULL,
                                        [unit_price] [decimal](18, 2) NULL,
                                        [total_amount] [decimal](18, 2) NULL)";

            using (var repos = new GenericRepository<DimProductEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(dimProductScript);
            }
            using (var repos = new GenericRepository<DimCustomerEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(dimCustomerScript);
            }
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(factSalesScript);
            }
        }

        [Fact]
        public void Test71_RelationalColumn_InsertDimensionData()
        {
            var product1 = new DimProductEntity { ProductName = "Notebook Dell", Category = "Electronics", Price = 4500m };
            var product2 = new DimProductEntity { ProductName = "Mouse Logitech", Category = "Peripherals", Price = 150m };

            using (var repos = new GenericRepository<DimProductEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(product1);
                repos.AddSync(product2);
            }

            var customer1 = new DimCustomerEntity { CustomerName = "Joao Silva", Region = "Southeast" };
            var customer2 = new DimCustomerEntity { CustomerName = "Maria Santos", Region = "Northeast" };

            using (var repos = new GenericRepository<DimCustomerEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(customer1);
                repos.AddSync(customer2);
            }
        }

        [Fact]
        public void Test72_RelationalColumn_InsertFactData()
        {
            var sales = new FactSalesEntity
            {
                SaleDate = "2024-01-15",
                ProductId = 1,
                CustomerId = 1,
                Quantity = 2,
                UnitPrice = 4500m,
                TotalAmount = 9000m
            };

            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(sales);
            }
        }

        [Fact]
        public void Test73_RelationalColumn_QueryWithJoin()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesEntity();
                var result = repos.QuerySync(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());

                var first = result.First();
                Assert.False(string.IsNullOrEmpty(first.ProductName));
                Assert.False(string.IsNullOrEmpty(first.CustomerName));
            }
        }

        [Fact]
        public void Test74_RelationalColumnQueryBuilder_OrderBy()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesEntity();
                var result = repos.QueryBuilder(filter).OrderBy("ProductName").ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        #endregion

        #region DataAggregationColumn Tests

        [Fact]
        public void Test75_DataAggregationColumn_QueryWithAggregations()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesEntity();
                var result = repos.QuerySync(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());

                var first = result.First();
                Assert.True(first.SumTotalAmount > 0);
                Assert.True(first.CountSales > 0);
                Assert.True(first.AvgUnitPrice > 0);
            }
        }

        [Fact]
        public void Test76_DataAggregationColumn_QueryBuilder_WithAggregations()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesEntity();
                var result = repos.QueryBuilder(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());

                var first = result.First();
                Assert.True(first.SumTotalAmount > 0);
                Assert.True(first.CountSales > 0);
            }
        }

        [Fact]
        public void Test77_DataAggregationColumn_MaxMin()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesEntity();
                var result = repos.QuerySync(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());

                var first = result.First();
                Assert.True(first.MaxTotalAmount > 0);
                Assert.True(first.MinTotalAmount > 0);
                Assert.True(first.MaxTotalAmount >= first.MinTotalAmount);
            }
        }

        #endregion

        #region Real Grouping and Sorting Tests

        [Fact]
        public void Test78_GroupSort_SetupFactData()
        {
            var row1 = new FactSalesEntity { SaleDate = "2024-02-01", ProductId = 1, CustomerId = 1, Quantity = 2, UnitPrice = 4500m, TotalAmount = 9000m };
            var row2 = new FactSalesEntity { SaleDate = "2024-02-02", ProductId = 2, CustomerId = 1, Quantity = 5, UnitPrice = 150m, TotalAmount = 750m };
            var row3 = new FactSalesEntity { SaleDate = "2024-02-03", ProductId = 1, CustomerId = 2, Quantity = 3, UnitPrice = 4500m, TotalAmount = 13500m };

            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(row1);
                repos.AddSync(row2);
                repos.AddSync(row3);
            }
        }

        [Fact]
        public void Test79_MainMethod_QuerySync_GroupBy_ProductId()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(new FactSalesEntity(), groupAttributes: "ProductId");
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);

                var product1 = result.First(r => r.ProductId == 1);
                Assert.Equal(31500m, product1.SumTotalAmount);
                Assert.Equal(3, product1.CountSales);

                var product2 = result.First(r => r.ProductId == 2);
                Assert.Equal(750m, product2.SumTotalAmount);
                Assert.Equal(1, product2.CountSales);
            }
        }

        [Fact]
        public void Test80_MainMethod_Query_GroupBy_ProductId()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.Query(new FactSalesEntity(), groupAttributes: "ProductId").GetAwaiter().GetResult();
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);

                var product2 = result.First(r => r.ProductId == 2);
                Assert.Equal(750m, product2.SumTotalAmount);
                Assert.Equal(1, product2.CountSales);
            }
        }

        [Fact]
        public void Test81_b_GroupSort_SetupSampleData()
        {
            var names = new[] { "Alpha Souza", "Beta Lima", "Gamma Costa", "Delta Rocha" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                foreach (var name in names)
                {
                    repos.AddSync(new SampleEntity
                    {
                        DocNumber = 99000 + names.ToList().IndexOf(name),
                        CreationDate = DateTime.Now,
                        Name = name,
                        Resume = "sort test group",
                        Age = 30,
                        Active = true
                    });
                }
            }
        }

        private List<SampleEntity> SortFilter()
        {
            var filter = new SampleEntity { Resume = "sort test group", Active = true };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                return repos.QuerySync(filter, filterConjunction: true).ToList();
            }
        }

        [Fact]
        public void Test82_MainMethod_QuerySync_OrderBy_Ascending()
        {
            var expected = new[] { "Alpha Souza", "Beta Lima", "Delta Rocha", "Gamma Costa" };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Resume = "sort test group", Active = true };
                var result = repos.QuerySync(filter, filterConjunction: true, sortAttributes: "Name");
                var list = result.ToList();
                Assert.Equal(4, list.Count);
                for (int i = 0; i < expected.Length; i++)
                    Assert.Equal(expected[i], list[i].Name);
            }
        }

        [Fact]
        public void Test83_MainMethod_QuerySync_OrderBy_Descending()
        {
            var expected = new[] { "Gamma Costa", "Delta Rocha", "Beta Lima", "Alpha Souza" };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Resume = "sort test group", Active = true };
                var result = repos.QuerySync(filter, filterConjunction: true, sortAttributes: "Name", orderDescending: true);
                var list = result.ToList();
                Assert.Equal(4, list.Count);
                for (int i = 0; i < expected.Length; i++)
                    Assert.Equal(expected[i], list[i].Name);
            }
        }

        [Fact]
        public void Test84_Builder_GroupBy_ProductId()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QueryBuilder(new FactSalesEntity()).GroupBy("ProductId").ToList();
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);

                var product1 = result.First(r => r.ProductId == 1);
                Assert.Equal(31500m, product1.SumTotalAmount);
                Assert.Equal(3, product1.CountSales);

                var product2 = result.First(r => r.ProductId == 2);
                Assert.Equal(750m, product2.SumTotalAmount);
            }
        }

        [Fact]
        public void Test85_Builder_OrderBy_Ascending_Real()
        {
            var expected = new[] { "Alpha Souza", "Beta Lima", "Delta Rocha", "Gamma Costa" };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Resume = "sort test group", Active = true };
                var result = repos.QueryBuilder(filter, filterConjunction: true).OrderBy("Name").ToList();
                var list = result.ToList();
                Assert.Equal(4, list.Count);
                for (int i = 0; i < expected.Length; i++)
                    Assert.Equal(expected[i], list[i].Name);
            }
        }

        [Fact]
        public void Test86_Builder_OrderBy_Descending_Real()
        {
            var expected = new[] { "Gamma Costa", "Delta Rocha", "Beta Lima", "Alpha Souza" };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Resume = "sort test group", Active = true };
                var result = repos.QueryBuilder(filter, filterConjunction: true).OrderBy("Name").Descending().ToList();
                var list = result.ToList();
                Assert.Equal(4, list.Count);
                for (int i = 0; i < expected.Length; i++)
                    Assert.Equal(expected[i], list[i].Name);
            }
        }

        #endregion
    }
}
