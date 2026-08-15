using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Rochas.Data.Specification.Enums;
using Rochas.Data.Specification.Models;
using Rochas.Extensions;

namespace Rochas.DapperRepository.Test
{
    [TestCaseOrderer("Rochas.DapperRepository.Test.AlphabeticalOrderer", "Rochas.DapperRepository.Test")]
    public class GenericRepositoryTest
    {
        private string databaseFileName = "MockDatabase.sqlite";
        private string connString = "Data Source=MockDatabase.sqlite;Mode=ReadWriteCreate;";

        #region Mock Repository Initialization

        [Fact]
        public void Test001_Initialize()
        {
            var tableScript = @"CREATE TABLE [sample_entity](
                                             [id] INTEGER PRIMARY KEY,
                                             [child_id] [int] NULL,
                                             [parent_id] [int] NULL,
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

            var guidTableScript = @"CREATE TABLE [sample_guid_entity] (
                                               [id] [varchar](36) PRIMARY KEY,
                                               [name] [varchar](200) NULL,
                                               [active] [bit] NOT NULL)";

            var guidArrayTableScript = @"CREATE TABLE [sample_guid_array_entity] (
                                                   [id] [varchar](36) PRIMARY KEY,
                                                   [name] [varchar](200) NULL,
                                                   [tags] [text] NULL,
                                                   [hash_codes] [text] NULL,
                                                   [active] [bit] NOT NULL)";

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(tableScript, databaseFileName);
                repos.Initialize(oneForeignTableScript);
                repos.Initialize(manyForeignTableScript);
                repos.Initialize(intermedyForeignTableScript);
                repos.Initialize(guidTableScript);
                repos.Initialize(guidArrayTableScript);
            }
        }

        #endregion

        #region Single Entity Tests

        [Fact]
        public void Test002_Add()
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
        public void Test003_AddComposition()
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
        public void Test004_GetByKey()
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
        public void Test005_GetByFilter()
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
        public void Test006_Query()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "roberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter).ToList();
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test007_QueryLimited()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "roberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter).ToList();
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test008_QuerySorted()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter).OrderBy(new[] { "Name" }).ToList();
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
            Assert.StartsWith("Gustavo", result.First().Name);

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter).OrderByDescending(new[] { "Name" }).ToList();
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
            Assert.StartsWith("Roberto", result.First().Name);
        }

        [Fact]
        public void Test009_QueryByDateRange()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity()
            {
                CreationDate = DateTime.Now.Date.AddDays(-1),
                CreationDateEnd = DateTime.Now.Date.AddDays(1)
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter, filterConjunction: false).ToList();
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test010_QueryByAgeMajorThan()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity()
            {
                Age = 16
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter).ToList();
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test011_Search()
        {
            ICollection<SampleEntity> result;

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var                 criteria = "torres";

                result = repos.SearchSync(criteria).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());

                criteria = "sao paulo";

                result = repos.SearchSync(criteria).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test012_BulkSearch()
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
        public void Test013_Count()
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
        public void Test014_Update()
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
        public void Test015_UpdateComposition()
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
        public void Test016_Remove()
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
        public void Test040_SearchPaginated()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(new SampleEntity { DocNumber = 90001, CreationDate = DateTime.Now, Name = "Paginated Alpha", Resume = "Test pagination", Age = 20, Active = true });
                repos.AddSync(new SampleEntity { DocNumber = 90002, CreationDate = DateTime.Now, Name = "Paginated Beta", Resume = "Test pagination", Age = 25, Active = true });
                repos.AddSync(new SampleEntity { DocNumber = 90003, CreationDate = DateTime.Now, Name = "Paginated Gamma", Resume = "Test pagination", Age = 30, Active = true });
            }

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchSync("paginated", 1, 2, filterConjunction: false).ToList();

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.True(result.Items.Count <= 2);
                Assert.True(result.TotalCount >= 2);
            }
        }

        [Fact]
        public void Test041_QueryPaginated()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QuerySync(filter, 1, 5, filterConjunction: false).ToList();

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.True(result.Items.Count <= 5);
                Assert.True(result.TotalCount > 0);
            }
        }

        [Fact]
        public void Test042_SearchPaginated_Async()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchSync("paginated", 1, 10, filterConjunction: false).ToList();

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.True(result.TotalCount >= 1);
            }
        }

        [Fact]
        public void Test043_QueryPaginated_Async()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QuerySync(filter, 1, 5, filterConjunction: false).ToList();

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.True(result.TotalCount > 0);
            }
        }

        [Fact]
        public void Test044_SearchPaginated_WithSort()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchSync("paginated", 1, 10, filterConjunction: false).OrderByDescending(new[] { "Name" }).ToList();

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.StartsWith("Paginated G", result.Items.First().Name);
            }
        }

        [Fact]
        public void Test045_QueryPaginated_WithSort()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QuerySync(filter, 1, 10, filterConjunction: false).OrderBy(new[] { "Name" }).ToList();

                Assert.NotNull(result);
                Assert.True(result.Items.Any());
            }
        }

        #endregion

        #region OneToOne Composite Entity Tests

        [Fact]
        public void Test024_OneToOneCompositionAdd()
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
        public void Test025_GetOneToOneCompositionByKey()
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
        public void Test026_QueryOneToOneComposition()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "alberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter, true).ToList();
            }

            Assert.NotNull(result);
            Assert.True(result.Any());

            Assert.NotNull(result.First().OneToOneForeignEntity);
        }

        #endregion

        #region ManyToOne Composition Entity Tests

        [Fact]
        public void Test027_ManyToOneCompositionAdd()
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
        public void Test028_GetManyToOneCompositionByKey()
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
        public void Test029_QueryManyToOneComposition()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "claudia" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter, true).ToList();
            }

            Assert.NotNull(result);
            Assert.True(result.Any());

            Assert.NotNull(result.First().ManyToOneForeignEntity);
        }

        #endregion

        #region OneToMany Composite Entity Tests

        [Fact]
        public void Test030_OneToManyCompositionAdd()
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
        public void Test031_GetOneToManyCompositionByKey()
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
        public void Test032_QueryOneToManyComposition()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "carlos" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.QuerySync(filter, true).ToList();
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
        public void Test033_IntermedyCompositionCreate()
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
        public void Test034_IntermedyCompositionGet()
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
        public void Test035_AddRange()
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
        public void Test036_GetByKey_EmptyKey_ReturnsNull()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.GetSync(0);
                Assert.Null(result);
            }
        }

        [Fact]
        public void Test037_GetByKey_NonExistentKey_ReturnsNull()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.GetSync(99999);
                Assert.Null(result);
            }
        }

        [Fact]
        public void Test038_SearchPaginated_PageSizeOne()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchSync("almeida", 1, 1, filterConjunction: false).ToList();
                Assert.NotNull(result);
                Assert.True(result.Items.Count <= 1);
                Assert.True(result.TotalCount >= 1);
            }
        }

        [Fact]
        public void Test039_QueryPaginated_EmptyFilter()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(new SampleEntity(), 1, 10, filterConjunction: false).ToList();
                Assert.NotNull(result);
                Assert.True(result.Items.Any());
            }
        }

        #endregion

        #region v1.6.8 Fixes - End-to-End Tests

        [Fact]
        public void Test046_EmptyStringFilter_ShouldNotAffectQuery()
        {
            var filter = new SampleEntity() { Name = "" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test049_NullStringFilter_ShouldNotAffectQuery()
        {
            var filter = new SampleEntity() { Name = null, Resume = null };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test050_Count_EmptyStringFilter()
        {
            var filter = new SampleEntity() { Name = "" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var count = repos.CountSync(filter);
                Assert.True(count > 0);
            }
        }

        [Fact]
        public void Test051_MixedFilters_EmptyAndNonEmpty()
        {
            var filter = new SampleEntity() { Name = "", Active = true };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
                Assert.All(result, e => Assert.True(e.Active));
            }
        }

        [Fact]
        public void Test052_DateRange_EndToEnd()
        {
            var filter = new SampleEntity()
            {
                CreationDate = DateTime.Now.Date.AddDays(-30),
                CreationDateEnd = DateTime.Now.Date.AddDays(1)
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter, filterConjunction: false).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test053_ArrayProperty_Insert_PersistsCSV()
        {
            var tableScript = @"DROP TABLE IF EXISTS [sample_array_entity];
                                CREATE TABLE IF NOT EXISTS [sample_array_entity] (
                                        [id] INTEGER PRIMARY KEY,
                                        [name] [varchar](200) NULL,
                                        [hash_codes] TEXT NULL,
                                        [tags] TEXT NULL,
                                        [blob_data] TEXT NULL,
                                        [active] [bit] NOT NULL)";

            using (var repos = new GenericRepository<SampleArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(tableScript);
            }

            var entity = new SampleArrayEntity()
            {
                Name = "array test",
                HashCodes = new uint[] { 12345, 67890 },
                Tags = new string[] { "alpha", "beta", "gama" },
                BlobData = new byte[] { 0x01, 0x02, 0xFE, 0xFF, 0x00 },
                Active = true
            };

            int id;
            using (var repos = new GenericRepository<SampleArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                id = repos.AddSync(entity);
            }

            Assert.True(id > 0);

            using (var repos = new GenericRepository<SampleArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(new SampleArrayEntity { Id = id }).ToList();
                Assert.NotNull(result);
                var loaded = result.FirstOrDefault();
                Assert.NotNull(loaded);
                Assert.NotNull(loaded.HashCodes);
                Assert.Equal(new uint[] { 12345, 67890 }, loaded.HashCodes);
                Assert.NotNull(loaded.Tags);
                Assert.Equal(new string[] { "alpha", "beta", "gama" }, loaded.Tags);
                Assert.NotNull(loaded.BlobData);
                Assert.Equal(new byte[] { 0x01, 0x02, 0xFE, 0xFF, 0x00 }, loaded.BlobData);
            }
        }

        [Fact]
        public void Test054_ArrayProperty_Query_ReadsCSV()
        {
            var entity = new SampleArrayEntity()
            {
                Name = "query array test",
                HashCodes = new uint[] { 11111, 22222 },
                Tags = new string[] { "x1", "x2" },
                BlobData = new byte[] { 0x0A, 0x0B, 0x0C },
                Active = true
            };

            using (var repos = new GenericRepository<SampleArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            var filter = new SampleArrayEntity() { Name = "query array test" };

            using (var repos = new GenericRepository<SampleArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
                Assert.Equal("query array test", result.First().Name);
                Assert.NotNull(result.First().HashCodes);
                Assert.Equal(new uint[] { 11111, 22222 }, result.First().HashCodes);
                Assert.NotNull(result.First().Tags);
                Assert.Equal(new string[] { "x1", "x2" }, result.First().Tags);
                Assert.NotNull(result.First().BlobData);
                Assert.Equal(new byte[] { 0x0A, 0x0B, 0x0C }, result.First().BlobData);
            }
        }

        [Fact]
        public void Test054b_ArrayProperty_Filter_IgnoresArrays()
        {
            // Filtro com array preenchido nÃ£o deve gerar condiÃ§Ã£o WHERE espÃºria.
            var filter = new SampleArrayEntity()
            {
                Name = "query array test",
                HashCodes = new uint[] { 999, 888 },
                Tags = new string[] { "zz" },
                BlobData = new byte[] { 0x01 }
            };

            using (var repos = new GenericRepository<SampleArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        #endregion

        #region Guid Primary Key Tests

        [Fact]
        public void Test054c_GuidPk_Add_AutoGeneratesGuid()
        {
            var entity = new SampleGuidEntity()
            {
                Name = "guid auto test",
                Active = true
            };

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            Assert.NotEqual(Guid.Empty, entity.Id);
        }

        [Fact]
        public void Test054d_GuidPk_GetById_ReturnsEntity()
        {
            var entity = new SampleGuidEntity()
            {
                Name = "guid get test",
                Active = true
            };

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                var loaded = repos.GetSync(entity.Id);
                Assert.NotNull(loaded);
                Assert.Equal(entity.Id, loaded.Id);
                Assert.Equal("guid get test", loaded.Name);
            }
        }

        [Fact]
        public void Test054e_GuidPk_Query_ByFilter_ReturnsEntity()
        {
            var entity = new SampleGuidEntity()
            {
                Name = "guid query test",
                Active = true
            };

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            var filter = new SampleGuidEntity() { Id = entity.Id };

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
                Assert.Equal(entity.Id, result.First().Id);
                Assert.Equal("guid query test", result.First().Name);
            }
        }

        [Fact]
        public void Test054f_GuidPk_Update_ById()
        {
            var entity = new SampleGuidEntity()
            {
                Name = "guid before update",
                Active = true
            };

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            var filter = new SampleGuidEntity() { Id = entity.Id };
            entity.Name = "guid after update";

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.UpdateSync(entity, filter);
            }

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                var loaded = repos.GetSync(entity.Id);
                Assert.NotNull(loaded);
                Assert.Equal("guid after update", loaded.Name);
            }
        }

        [Fact]
        public void Test054g_GuidPk_Delete_ById()
        {
            var entity = new SampleGuidEntity()
            {
                Name = "guid delete test",
                Active = true
            };

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            var filter = new SampleGuidEntity() { Id = entity.Id };

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.RemoveSync(filter);
            }

            using (var repos = new GenericRepository<SampleGuidEntity>(DatabaseEngine.SQLite, connString))
            {
                var loaded = repos.GetSync(entity.Id);
                Assert.Null(loaded);
            }
        }

        #endregion

        #region Guid + Array Search Tests

        [Fact]
        public void Test054h_GuidArray_Add_PersistsArrays()
        {
            var entity = new SampleGuidArrayEntity()
            {
                Name = "guid array test",
                Tags = new string[] { "alpha", "beta", "gama" },
                HashCodes = new uint[] { 100, 200 },
                Active = true
            };

            using (var repos = new GenericRepository<SampleGuidArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            Assert.NotEqual(Guid.Empty, entity.Id);
        }

        [Fact]
        public void Test054i_GuidArray_Get_ReadsArrays()
        {
            var entity = new SampleGuidArrayEntity()
            {
                Name = "guid array get",
                Tags = new string[] { "x", "y" },
                HashCodes = new uint[] { 777 },
                Active = true
            };

            using (var repos = new GenericRepository<SampleGuidArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            using (var repos = new GenericRepository<SampleGuidArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                var loaded = repos.GetSync(entity.Id);
                Assert.NotNull(loaded);
                Assert.Equal(entity.Id, loaded.Id);
                Assert.Equal(new string[] { "x", "y" }, loaded.Tags);
                Assert.Equal(new uint[] { 777 }, loaded.HashCodes);
            }
        }

        [Fact]
        public void Test054j_GuidArray_Search_ByTags_Like()
        {
            var entity = new SampleGuidArrayEntity()
            {
                Name = "search by tags",
                Tags = new string[] { "red", "blue", "green" },
                HashCodes = new uint[] { 1 },
                Active = true
            };

            using (var repos = new GenericRepository<SampleGuidArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            using (var repos = new GenericRepository<SampleGuidArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchSync("blue").ToList();
                Assert.NotNull(result);
                Assert.True(result.Any(r => r.Id == entity.Id));
            }
        }

        [Fact]
        public void Test054k_GuidArray_Search_ByTags_MatchesSingleElement()
        {
            var entity1 = new SampleGuidArrayEntity()
            {
                Name = "multi tags a",
                Tags = new string[] { "fast", "slow" },
                Active = true
            };
            var entity2 = new SampleGuidArrayEntity()
            {
                Name = "multi tags b",
                Tags = new string[] { "fast", "quiet" },
                Active = true
            };

            using (var repos = new GenericRepository<SampleGuidArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity1);
                repos.AddSync(entity2);
            }

            using (var repos = new GenericRepository<SampleGuidArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchSync("fast").ToList();
                Assert.Equal(2, result.Count);
            }
        }

        [Fact]
        public void Test054l_GuidArray_Search_ByTags_NoMatch_ReturnsEmpty()
        {
            var entity = new SampleGuidArrayEntity()
            {
                Name = "no match",
                Tags = new string[] { "one", "two" },
                Active = true
            };

            using (var repos = new GenericRepository<SampleGuidArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            using (var repos = new GenericRepository<SampleGuidArrayEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchSync("nonexistent").ToList();
                Assert.True(result.All(r => r.Id != entity.Id));
            }
        }

        #endregion

        #region QueryBuilder Tests

        [Fact]
        public async Task Test055_QueryBuilder_FilterOnly()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = await repos.Query(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public async Task Test056_QueryBuilder_FilterOnly_Await()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = await repos.Query(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public async Task Test057_QueryBuilder_OrderBy_SingleColumn()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = await repos.Query(filter).OrderBy(new[] { "Name" });
                Assert.NotNull(result);
                Assert.True(result.Count > 1);
                var list = result.ToList();
                Assert.True(list[0].Name.CompareTo(list[1].Name) <= 0);
            }
        }

        [Fact]
        public async Task Test058_QueryBuilder_OrderBy_Descending()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = await repos.Query(filter).OrderByDescending(new[] { "Name" });
                Assert.NotNull(result);
                Assert.True(result.Count > 1);
                var list = result.ToList();
                Assert.True(list[0].Name.CompareTo(list[1].Name) >= 0);
            }
        }

        [Fact]
        public async Task Test059_QueryBuilder_OrderBy_MultipleColumns()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = await repos.Query(filter).OrderBy(new[] { "Age", "Name" });
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public async Task Test060_QueryBuilder_OrderBy_Await()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = await repos.Query(filter).OrderBy(new[] { "Name" });
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public async Task Test061_QueryBuilder_OrderBy_Descending_Await()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = await repos.Query(filter).OrderByDescending(new[] { "Name" });
                Assert.NotNull(result);
                Assert.True(result.Count > 1);
                var list = result.ToList();
                Assert.True(list[0].Name.CompareTo(list[1].Name) >= 0);
            }
        }

        [Fact]
        public async Task Test062_QueryBuilder_EmptyFilter()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity();
                var result = await repos.Query(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public async Task Test063_QueryBuilder_FilterConjunction()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true, Age = 32 };
                var result = await repos.Query(filter, filterConjunction: true).OrderBy(new[] { "Name" });
                Assert.NotNull(result);
            }
        }

        #endregion

        #region QueryPaginatedBuilder Tests

        [Fact]
        public void Test064_QueryPaginatedBuilder_Paginate()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QuerySync(filter, 1, 2, filterConjunction: false).ToList();
                Assert.NotNull(result);
                Assert.True(result.Items.Count <= 2);
                Assert.True(result.TotalCount > 0);
            }
        }

        [Fact]
        public void Test065_QueryPaginatedBuilder_Paginate_Page2()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QuerySync(filter, 2, 1, filterConjunction: false).ToList();
                Assert.NotNull(result);
                Assert.True(result.Items.Count <= 1);
                Assert.True(result.TotalCount > 0);
            }
        }

        [Fact]
        public void Test066_QueryPaginatedBuilder_OrderBy()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QuerySync(filter, 1, 10, filterConjunction: false).OrderBy(new[] { "Name" }).ToList();
                Assert.NotNull(result);
                Assert.True(result.Items.Count > 1);
                Assert.True(result.Items.ElementAt(0).Name.CompareTo(result.Items.ElementAt(1).Name) <= 0);
            }
        }

        [Fact]
        public void Test067_QueryPaginatedBuilder_OrderBy_Descending()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QuerySync(filter, 1, 10, filterConjunction: false).OrderByDescending(new[] { "Name" }).ToList();
                Assert.NotNull(result);
                Assert.True(result.Items.Count > 1);
                Assert.True(result.Items.First().Name.CompareTo(result.Items.ElementAt(1).Name) >= 0);
            }
        }

        [Fact]
        public void Test068_QueryPaginatedBuilder_OrderBy_Await()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QuerySync(filter, 1, 5, filterConjunction: false).OrderBy(new[] { "Name" }).ToList();
                Assert.NotNull(result);
                Assert.True(result.Items.Any());
            }
        }

        [Fact]
        public void Test069_QueryPaginatedBuilder_EmptyFilter()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity();
                var result = repos.QuerySync(filter, 1, 5, filterConjunction: false).ToList();
                Assert.NotNull(result);
                Assert.True(result.Items.Any());
                Assert.True(result.TotalCount > 0);
            }
        }

        #endregion

        #region RelationalColumn Tests

        [Fact]
        public void Test070_RelationalColumn_CreateTables()
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
        public void Test071_RelationalColumn_InsertDimensionData()
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
        public void Test072_RelationalColumn_InsertFactData()
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
        public void Test073_RelationalColumn_QueryWithJoin()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesEntity();
                var result = repos.QuerySync(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());

                var first = result.First();
                Assert.False(string.IsNullOrEmpty(first.ProductName));
                Assert.False(string.IsNullOrEmpty(first.CustomerName));
            }
        }

        [Fact]
        public async Task Test074_RelationalColumnQueryBuilder_OrderBy()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesEntity();
                var result = await repos.Query(filter).OrderBy(new[] { "ProductName" });
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        #endregion

        #region DataAggregationColumn Tests

        [Fact]
        public void Test075_DataAggregationColumn_QueryWithAggregations()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesEntity();
                var result = repos.QuerySync(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());

                var first = result.First();
                Assert.True(first.SumTotalAmount > 0);
                Assert.True(first.CountSales > 0);
                Assert.True(first.AvgUnitPrice > 0);
            }
        }

        [Fact]
        public async Task Test076_DataAggregationColumn_QueryBuilder_WithAggregations()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesEntity();
                var result = await repos.Query(filter);
                Assert.NotNull(result);
                Assert.True(result.Any());

                var first = result.First();
                Assert.True(first.SumTotalAmount > 0);
                Assert.True(first.CountSales > 0);
            }
        }

        [Fact]
        public void Test077_DataAggregationColumn_MaxMin()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesEntity();
                var result = repos.QuerySync(filter).ToList();
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
        public void Test078_GroupSort_SetupFactData()
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
        public void Test079_MainMethod_QuerySync_GroupBy_ProductId()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QuerySync(new FactSalesEntity()).GroupBy(new[] { "ProductId" }).ToList();
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
        public async Task Test080_MainMethod_Query_GroupBy_ProductId()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = await repos.Query(new FactSalesEntity()).GroupBy(new[] { "ProductId" });
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);

                var product2 = result.First(r => r.ProductId == 2);
                Assert.Equal(750m, product2.SumTotalAmount);
                Assert.Equal(1, product2.CountSales);
            }
        }

        [Fact]
        public void Test081_b_GroupSort_SetupSampleData()
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

        private ICollection<SampleEntity> SortFilter()
        {
            var filter = new SampleEntity { Resume = "sort test group", Active = true };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                return repos.QuerySync(filter, filterConjunction: true).ToList();
            }
        }

        [Fact]
        public void Test082_MainMethod_QuerySync_OrderBy_Ascending()
        {
            var expected = new[] { "Alpha Souza", "Beta Lima", "Delta Rocha", "Gamma Costa" };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Resume = "sort test group", Active = true };
                var result = repos.QuerySync(filter, filterConjunction: true).OrderBy(new[] { "Name" }).ToList();
                var list = result.ToList();
                Assert.Equal(4, list.Count);
                for (int i = 0; i < expected.Length; i++)
                    Assert.Equal(expected[i], list[i].Name);
            }
        }

        [Fact]
        public void Test083_MainMethod_QuerySync_OrderBy_Descending()
        {
            var expected = new[] { "Gamma Costa", "Delta Rocha", "Beta Lima", "Alpha Souza" };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Resume = "sort test group", Active = true };
                var result = repos.QuerySync(filter, filterConjunction: true).OrderByDescending(new[] { "Name" }).ToList();
                var list = result.ToList();
                Assert.Equal(4, list.Count);
                for (int i = 0; i < expected.Length; i++)
                    Assert.Equal(expected[i], list[i].Name);
            }
        }

        [Fact]
        public async Task Test084_Builder_GroupBy_ProductId()
        {
            using (var repos = new GenericRepository<FactSalesEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = await repos.Query(new FactSalesEntity()).GroupBy(new[] { "ProductId" });
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
        public async Task Test085_Builder_OrderBy_Ascending_Real()
        {
            var expected = new[] { "Alpha Souza", "Beta Lima", "Delta Rocha", "Gamma Costa" };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Resume = "sort test group", Active = true };
                var result = await repos.Query(filter, filterConjunction: true).OrderBy(new[] { "Name" });
                var list = result.ToList();
                Assert.Equal(4, list.Count);
                for (int i = 0; i < expected.Length; i++)
                    Assert.Equal(expected[i], list[i].Name);
            }
        }

        [Fact]
        public async Task Test086_Builder_OrderBy_Descending_Real()
        {
            var expected = new[] { "Gamma Costa", "Delta Rocha", "Beta Lima", "Alpha Souza" };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Resume = "sort test group", Active = true };
                var result = await repos.Query(filter, filterConjunction: true).OrderByDescending(new[] { "Name" });
                var list = result.ToList();
                Assert.Equal(4, list.Count);
                for (int i = 0; i < expected.Length; i++)
                    Assert.Equal(expected[i], list[i].Name);
            }
        }

        #endregion

        #region v1.7.6 Tests - Collection Exclusion, QueryRaw, FillComposition Fixes

        [Fact]
        public void Test087_InvoiceTable_Create()
        {
            var tableScript = @"CREATE TABLE IF NOT EXISTS [sample_invoice_entity] (
                [id] INTEGER PRIMARY KEY,
                [invoice_number] varchar(50) NOT NULL,
                [customer_id] int NOT NULL,
                [total_amount] decimal(18,2) NOT NULL,
                [active] bit NOT NULL)";

            var itemTableScript = @"CREATE TABLE IF NOT EXISTS [sample_invoice_item_entity] (
                [id] INTEGER PRIMARY KEY,
                [invoice_id] int NOT NULL,
                [product_name] varchar(200) NOT NULL,
                [quantity] int NOT NULL,
                [unit_price] decimal(18,2) NOT NULL,
                [line_total] decimal(18,2) NOT NULL)";

            using (var repos = new GenericRepository<SampleInvoiceEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(tableScript);
                repos.Initialize(itemTableScript);
            }
        }

        [Fact]
        public void Test088_Invoice_IReadOnlyCollection_Excluded_From_Insert()
        {
            using (var repos = new GenericRepository<SampleInvoiceEntity>(DatabaseEngine.SQLite, connString))
            {
                var invoice = new SampleInvoiceEntity
                {
                    InvoiceNumber = "INV-001",
                    CustomerId = 1,
                    Active = true
                };
                invoice.AddItem(new SampleInvoiceItemEntity
                {
                    ProductName = "Product A",
                    Quantity = 2,
                    UnitPrice = 50m,
                    LineTotal = 100m
                });
                invoice.AddItem(new SampleInvoiceItemEntity
                {
                    ProductName = "Product B",
                    Quantity = 1,
                    UnitPrice = 75m,
                    LineTotal = 75m
                });

                repos.AddSync(invoice);

                var loaded = repos.GetSync(invoice.Id);
                Assert.NotNull(loaded);
                Assert.Equal("INV-001", loaded.InvoiceNumber);
                Assert.Equal(1, loaded.CustomerId);
                Assert.Equal(175m, loaded.TotalAmount);
            }
        }

        [Fact]
        public void Test089_Invoice_IReadOnlyCollection_Not_In_Update_SQL()
        {
            using (var repos = new GenericRepository<SampleInvoiceEntity>(DatabaseEngine.SQLite, connString))
            {
                var invoice = new SampleInvoiceEntity
                {
                    InvoiceNumber = "INV-002",
                    CustomerId = 2,
                    Active = true
                };
                repos.AddSync(invoice);

                invoice.InvoiceNumber = "INV-002-UPDATED";
                var filter = new SampleInvoiceEntity { Id = invoice.Id };
                repos.UpdateSync(invoice, filter);

                var loaded = repos.GetSync(invoice.Id);
                Assert.NotNull(loaded);
                Assert.Equal("INV-002-UPDATED", loaded.InvoiceNumber);
            }
        }

        [Fact]
        public void Test090_InvoiceItem_Insert_And_Query()
        {
            using (var repos = new GenericRepository<SampleInvoiceItemEntity>(DatabaseEngine.SQLite, connString))
            {
                var item = new SampleInvoiceItemEntity
                {
                    InvoiceId = 1,
                    ProductName = "Widget",
                    Quantity = 5,
                    UnitPrice = 20m,
                    LineTotal = 100m
                };
                repos.AddSync(item);

                var loaded = repos.GetSync(item.Id);
                Assert.NotNull(loaded);
                Assert.Equal("Widget", loaded.ProductName);
                Assert.Equal(5, loaded.Quantity);
                Assert.Equal(100m, loaded.LineTotal);
            }
        }

        [Fact]
        public void Test091_QueryRaw_Select_All()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QueryRawSync("SELECT * FROM sample_entity WHERE active = 1", new Dictionary<string, object>());
                Assert.NotNull(result);
                Assert.True(result.Count > 0);
            }
        }

        [Fact]
        public void Test092_QueryRaw_With_Parameters()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var parameters = new Dictionary<string, object> { { "@name", "Alpha Souza" } };
                var result = repos.QueryRawSync("SELECT * FROM sample_entity WHERE name = @name", parameters);
                Assert.NotNull(result);
                Assert.Single(result);
                Assert.Equal("Alpha Souza", result.First().Name);
            }
        }

        [Fact]
        public void Test093_QueryRaw_Rejects_NON_Select()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                Assert.Throws<ArgumentException>(() =>
                    repos.QueryRawSync("DROP TABLE sample_entity", new Dictionary<string, object>()));
            }
        }

        [Fact]
        public void Test094_QueryRaw_Rejects_Semicolon()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                Assert.Throws<ArgumentException>(() =>
                    repos.QueryRawSync("SELECT * FROM sample_entity; DROP TABLE sample_entity", new Dictionary<string, object>()));
            }
        }

        [Fact]
        public void Test095_QueryRaw_Rejects_Comments()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                Assert.Throws<ArgumentException>(() =>
                    repos.QueryRawSync("SELECT * FROM sample_entity -- injection", new Dictionary<string, object>()));
            }
        }

        [Fact]
        public void Test096_QueryRaw_Rejects_Null_Parameters()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                Assert.Throws<ArgumentException>(() =>
                    repos.QueryRawSync("SELECT * FROM sample_entity", null));
            }
        }

        [Fact]
        public void Test097_FillComposition_NullFK_DoesNotNRE()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = repos.QuerySync(filter).ToList();
                Assert.NotNull(result);
                Assert.True(result.Count > 0);

                foreach (var entity in result)
                {
                    var loaded = repos.GetSync(entity.Id);
                    Assert.NotNull(loaded);
                    Assert.NotNull(loaded.Name);
                }
            }
        }

        [Fact]
        public async Task Test098_GroupBy_Builder_Complex()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Resume = "sort test group", Active = true };
                var result = await repos.Query(filter, filterConjunction: true)
                    .GroupBy(new[] { "Resume" });
                Assert.NotNull(result);
                Assert.True(result.Count >= 1);
            }
        }

        [Fact]
        public async Task Test099_OrderBy_Builder_MultiColumn()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = await repos.Query(filter)
                    .OrderBy(new[] { "Age", "Name" });
                Assert.NotNull(result);
                Assert.True(result.Count > 0);
            }
        }

        [Fact]
        public async Task Test100_Builder_GroupBy_OrderBy_Combined()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new SampleEntity { Active = true };
                var result = await repos.Query(filter)
                    .GroupBy(new[] { "Resume" })
                    .OrderBy(new[] { "Name" });
                Assert.NotNull(result);
            }
        }

        [Fact]
        public void Test101_FillComposition_CycleDetection_DoesNotOverflow()
        {
            // Create a sample entity with children that have a ManyToOne back-reference.
            // This creates a cyclic reference: SampleEntity â†” SampleManyForeignEntity
            // FillComposition must detect and break the cycle without stack overflow.
            var entity = new SampleEntity()
            {
                DocNumber = 99999,
                CreationDate = DateTime.Now,
                Name = "Cycle Test Entity",
                Active = true,
                OneToManyForeignEntities = new List<SampleManyForeignEntity>
                {
                    new SampleManyForeignEntity { Code = 1, Title = "Child 1", CreationDate = DateTime.Now, Active = true },
                    new SampleManyForeignEntity { Code = 2, Title = "Child 2", CreationDate = DateTime.Now, Active = true }
                }
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var rowsAffected = repos.AddSync(entity, persistComposition: true);

                Assert.True(rowsAffected > 0);

                // Reload with composition â€” should not stack overflow
                var result = repos.GetSync(entity.Id, loadComposition: true);

                Assert.NotNull(result);
                Assert.Equal(entity.Id, result.Id);
                Assert.NotNull(result.OneToManyForeignEntities);
                Assert.True(result.OneToManyForeignEntities.Count >= 1);

                foreach (var child in result.OneToManyForeignEntities)
                {
                    Assert.NotNull(child);
                    Assert.True(child.ParentId == result.Id);
                }
            }
        }

        [Fact]
        public void Test102_SelfManyToOne_HierarchicalTree_DoesNotOverflow()
        {
            // Self-referencing ManyToOne via ParentId (hierarchical tree).
            // Child.ParentId â†’ Parent.Id. Cycle tolerance + ParentId tree must not overflow.
            var parent = new SampleEntity()
            {
                DocNumber = 77701,
                CreationDate = DateTime.Now,
                Name = "Self Parent",
                Active = true
            };
            var child = new SampleEntity()
            {
                DocNumber = 77702,
                CreationDate = DateTime.Now,
                Name = "Self Child",
                Active = true
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(parent);
                child.ParentId = parent.Id;
                repos.AddSync(child);

                // Load child with composition â€” SelfReferencedEntity must resolve to the parent
                // (WHERE Id = child.ParentId) without stack overflow.
                var result = repos.GetSync(child.Id, loadComposition: true);

                Assert.NotNull(result);
                Assert.Equal(child.Id, result.Id);
                Assert.NotNull(result.SelfReferencedEntity);
                Assert.Equal(parent.Id, result.SelfReferencedEntity.Id);
                Assert.Equal("Self Parent", result.SelfReferencedEntity.Name);
            }
        }
        #endregion

        #region GroupBy Aggregates Dictionary Tests

        [Fact]
        public void Test103_GroupBy_Aggregates_Dictionary_SumAndCount()
        {
            using (var repos = new GenericRepository<FactSalesDictionaryEntity>(DatabaseEngine.SQLite, connString))
            {
                var filter = new FactSalesDictionaryEntity();
                var aggregates = new Dictionary<string, DataAggregationType>
                {
                    { "TotalAmount", DataAggregationType.Sum },
                    { "Quantity", DataAggregationType.Count }
                };

                var result = repos.QuerySync(filter).GroupBy(new[] { "ProductId" }, aggregates).ToList();

                Assert.NotNull(result);
                Assert.Equal(2, result.Count);

                var product1 = result.First(r => r.ProductId == 1);
                Assert.Equal(31500m, product1.TotalAmount);
                Assert.Equal(3, product1.Quantity);

                var product2 = result.First(r => r.ProductId == 2);
                Assert.Equal(750m, product2.TotalAmount);
                Assert.Equal(1, product2.Quantity);
            }
        }

        #endregion
    }
}
