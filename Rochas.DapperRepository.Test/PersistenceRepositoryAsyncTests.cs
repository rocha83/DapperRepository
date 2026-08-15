using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Rochas.Data.Specification.Enums;

namespace Rochas.DapperRepository.Test
{
    [TestCaseOrderer("Rochas.DapperRepository.Test.AlphabeticalOrderer", "Rochas.DapperRepository.Test")]
    public class PersistenceRepositoryAsyncTests
    {
        private string databaseFileName = "AsyncCoverageTests.sqlite";
        private string connString = "Data Source=AsyncCoverageTests.sqlite;Mode=ReadWriteCreate;";

        [Fact]
        public void Test301_AsyncRepository_Initialize()
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

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(tableScript, databaseFileName);
            }
        }

        [Fact]
        public async Task Test302_AsyncAdd()
        {
            var entity = new SampleEntity()
            {
                DocNumber = 71001,
                CreationDate = DateTime.Now,
                Name = "Async Insert",
                Resume = "Async coverage test",
                Age = 31,
                Active = true
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var id = await repos.Add(entity);
                Assert.True(id > 0);
            }
        }

        [Fact]
        public async Task Test303_AsyncGet_ByKey()
        {
            var entity = new SampleEntity()
            {
                DocNumber = 71002,
                CreationDate = DateTime.Now,
                Name = "Async Get By Key",
                Resume = "Async coverage test",
                Age = 32,
                Active = true
            };

            int newId;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                newId = await repos.Add(entity);
            }

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var loaded = await repos.Get(newId);
                Assert.NotNull(loaded);
                Assert.Equal("Async Get By Key", loaded.Name);
            }
        }

        [Fact]
        public async Task Test304_AsyncGet_ByFilter()
        {
            var entity = new SampleEntity()
            {
                DocNumber = 71003,
                CreationDate = DateTime.Now,
                Name = "Async Get By Filter",
                Resume = "Async coverage test",
                Age = 33,
                Active = true
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                await repos.Add(entity);
            }

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var loaded = await repos.Get(new SampleEntity { DocNumber = 71003 });
                Assert.NotNull(loaded);
                Assert.Equal("Async Get By Filter", loaded.Name);
            }
        }

        [Fact]
        public async Task Test305_AsyncUpdate()
        {
            var entity = new SampleEntity()
            {
                DocNumber = 71004,
                CreationDate = DateTime.Now,
                Name = "Async Update",
                Resume = "Async coverage test",
                Age = 34,
                Active = true
            };

            int newId;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                newId = await repos.Add(entity);
            }

            entity.Age = 44;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var affected = await repos.Update(entity, new SampleEntity { DocNumber = 71004 });
                Assert.Equal(1, affected);
            }

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var reloaded = await repos.Get(newId);
                Assert.NotNull(reloaded);
                Assert.Equal(44, reloaded.Age);
            }
        }

        [Fact]
        public async Task Test306_AsyncRemove()
        {
            var entity = new SampleEntity()
            {
                DocNumber = 71005,
                CreationDate = DateTime.Now,
                Name = "Async Remove",
                Resume = "Async coverage test",
                Age = 35,
                Active = true
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                await repos.Add(entity);
            }

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var affected = await repos.Remove(new SampleEntity { DocNumber = 71005 });
                Assert.Equal(1, affected);
            }
        }

        [Fact]
        public async Task Test307_AsyncCount()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var count = await repos.Count(new SampleEntity { Active = true });
                Assert.True(count > 0);
            }
        }

        [Fact]
        public async Task Test308_AsyncAddRange()
        {
            var list = new List<SampleEntity>()
            {
                new SampleEntity() { DocNumber = 71006, CreationDate = DateTime.Now, Name = "Async Range One", Resume = "Async coverage test", Age = 36, Active = true },
                new SampleEntity() { DocNumber = 71007, CreationDate = DateTime.Now, Name = "Async Range Two", Resume = "Async coverage test", Age = 37, Active = true }
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                await repos.AddRange(list);
            }

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var one = await repos.Get(new SampleEntity { DocNumber = 71006 });
                var two = await repos.Get(new SampleEntity { DocNumber = 71007 });
                Assert.NotNull(one);
                Assert.NotNull(two);
            }
        }

        [Fact]
        public async Task Test309_AsyncSearch_Paginated()
        {
            var list = new List<SampleEntity>()
            {
                new SampleEntity() { DocNumber = 71008, CreationDate = DateTime.Now, Name = "AsyncToken Alpha", Resume = "Async coverage test", Age = 38, Active = true },
                new SampleEntity() { DocNumber = 71009, CreationDate = DateTime.Now, Name = "AsyncToken Beta", Resume = "Async coverage test", Age = 39, Active = true },
                new SampleEntity() { DocNumber = 71010, CreationDate = DateTime.Now, Name = "AsyncToken Gamma", Resume = "Async coverage test", Age = 40, Active = true }
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                await repos.AddRange(list);
            }

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = await repos.Search("AsyncToken", 1, 2, sortAttributes: "Name");
                Assert.NotNull(result);
                Assert.Equal(2, result.Items.Count);
                Assert.True(result.TotalCount >= 3);
                Assert.Equal(2, result.PageCount);
            }
        }

        [Fact]
        public async Task Test310_AsyncQueryRaw_Paginated()
        {
            var parameters = new Dictionary<string, object> { { "@name", "Async Insert" } };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = await repos.QueryRaw(
                    "SELECT * FROM sample_entity WHERE name = @name",
                    "SELECT COUNT(*) FROM sample_entity WHERE name = @name",
                    parameters, 1, 5);

                Assert.NotNull(result);
                Assert.NotEmpty(result.Items);
                Assert.True(result.TotalCount >= 1);
                Assert.Equal("Async Insert", result.Items.First().Name);
            }
        }

        [Fact]
        public async Task Test311_AsyncGet_EmptyKey_ReturnsNull()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = await repos.Get(0);
                Assert.Null(result);
            }
        }

        [Fact]
        public void Test312_AsyncQuerySync_Count_Builder()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var count = repos.QuerySync(new SampleEntity { Active = true }, filterConjunction: false).Count();
                Assert.True(count > 0);
            }
        }

        [Fact]
        public void Test313_AsyncBulkSearch()
        {
            var entity = new SampleEntity()
            {
                DocNumber = 71011,
                CreationDate = DateTime.Now,
                Name = "Bulky Coverage",
                Resume = "Async coverage test",
                Age = 41,
                Active = true
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.AddSync(entity);
            }

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.BulkSearch(new object[] { "Bulky" });
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test314_AsyncBulkSearchSync()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.BulkSearchSync(new object[] { "Bulky" }, recordsLimit: 1, sortAttributes: "Name");
                Assert.NotNull(result);
                Assert.True(result.Count <= 1);
            }
        }

        [Fact]
        public void Test316_AsyncSearchSync_Builder()
        {
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.SearchSync("Async Insert").OrderBy(new[] { "Name" }).ToList();
                Assert.NotNull(result);
                Assert.True(result.Any());
            }
        }

        [Fact]
        public void Test317_AsyncQueryRawSync_Paginated()
        {
            var parameters = new Dictionary<string, object> { { "@name", "Async Insert" } };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var result = repos.QueryRawSync(
                    "SELECT * FROM sample_entity WHERE name = @name",
                    "SELECT COUNT(*) FROM sample_entity WHERE name = @name",
                    parameters, 1, 5);

                Assert.NotNull(result);
                Assert.NotEmpty(result.Items);
            }
        }
    }
}