using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Rochas.DapperRepository.Specification.Enums;

namespace Rochas.DapperRepository.Test
{
    [TestCaseOrderer("Rochas.DapperRepository.Test.AlphabeticalOrderer", "Rochas.DapperRepository.Test")]
    public class GenericRepositoryTest
    {
        private string databaseFileName = "MockDatabase.sqlite";
        private string connString = "Data Source=MockDatabase.sqlite;Version=3;New=True;";

        #region Single Entity Tests

        [Fact]
        public void Test01_Initialize()
        {
            var tableScript = @"CREATE TABLE [sample_entity](
                                             [id] INTEGER PRIMARY KEY,
                                             [doc_number] [int] NOT NULL,
	                                         [creation_date] [datetime] NOT NULL,
	                                         [name] [varchar](200) NOT NULL,
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
                                                        [parent_id] [int] NOT NULL,
                                                        [creation_date] [datetime] NOT NULL,
                                                        [title] [varchar](100) NOT NULL,
	                                                    [description] [varchar](400) NOT NULL,
                                                        [active] [bit] NOT NULL)";

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(tableScript, databaseFileName);
                repos.Initialize(oneForeignTableScript);
                repos.Initialize(manyForeignTableScript);
            }
        }

        [Fact]
        public void Test02_Create()
        {
            int result;
            var sampleEntity = new SampleEntity()
            {
                DocNumber = 12345,
                CreationDate = DateTime.Now,
                Name = "Roberto Torres",
                Active = true
            };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.CreateSync(sampleEntity);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test03_GetByKey()
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
        public void Test04_GetByFilter()
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
        public void Test05_List()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "roberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test06_ListLimited()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "roberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter, recordsLimit: 5);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
            Assert.True(result.Count <= 5);
        }

        [Fact]
        public void Test07_ListByDateRange()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { CreationDate = DateTime.Now.Date.AddDays(-1),
                                              CreationDateEnd = DateTime.Now.Date.AddDays(1) };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test08_Search()
        {
            ICollection<SampleEntity> result;

            var filter = "torres";

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.SearchSync(filter);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test09_Count()
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
        public void Test10_Edit()
        {
            int result = 0;
            var filter = new SampleEntity() { DocNumber = 12345 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var item = repos.GetSync(filter);
                if (item != null)
                {
                    item.Age = 37;
                    result = repos.EditSync(item, filter);
                }
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test11_Delete()
        {
            int result = 0;
            var filter = new SampleEntity() { DocNumber = 12345 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.DeleteSync(filter);
            }

            Assert.True(result > 0);
        }

        #endregion

        #region Composite Entity Tests

        [Fact]
        public void Test12_OneCompositionCreate()
        {
            int result;
            var sampleEntity = new SampleEntity()
            {
                DocNumber = 13456,
                CreationDate = DateTime.Now,
                Name = "Alberto Gomes",
                Active = true,
                OneForeignEntity = new SampleOneForeignEntity()
                {
                    Title = "Titulo Teste Singular",
                    Description = "Descricao Teste Lorem Ipsum Lorem Ipsum"
                }
            };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.CreateSync(sampleEntity, true);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test13_GetOneCompositionByKey()
        {
            SampleEntity result;

            var key = 1;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.GetSync(key, true);
            }

            Assert.NotNull(result);
            Assert.Equal(key, result.Id);

            Assert.NotNull(result.OneForeignEntity);
        }

        [Fact]
        public void Test14_ListOneComposition()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "alberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter, true);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());

            Assert.NotNull(result.First().OneForeignEntity);
        }

        [Fact]
        public void Test15_ManyCompositionCreate()
        {
            int result;
            var sampleEntity = new SampleEntity()
            {
                DocNumber = 13456,
                CreationDate = DateTime.Now,
                Name = "Carlos Almeida",
                Active = true,
                ManyForeignEntities = new List<SampleManyForeignEntity>()
            };

            for(var counter = 1; counter < 6; counter++)
            sampleEntity.ManyForeignEntities.Add(new SampleManyForeignEntity()
            {
                CreationDate = sampleEntity.CreationDate,
                Title = $"Titulo Teste Plural {counter}",
                Description = $"Descricao Item {counter} Lorem Ipsum Lorem Ipsum",
                Active = true
            });

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.CreateSync(sampleEntity, true);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test16_GetManyCompositionByKey()
        {
            SampleEntity result;

            var key = 2;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.GetSync(key, true);
            }

            Assert.NotNull(result);
            Assert.Equal(key, result.Id);

            Assert.NotNull(result.ManyForeignEntities);
            Assert.True(result.ManyForeignEntities.Count == 5);
        }

        [Fact]
        public void Test17_ListManyComposition()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "carlos" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter, true);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());

            var firstItem = result.First();
            Assert.NotNull(firstItem.ManyForeignEntities);
            Assert.True(firstItem.ManyForeignEntities.Count == 5);
        }

        #endregion
    }
}
