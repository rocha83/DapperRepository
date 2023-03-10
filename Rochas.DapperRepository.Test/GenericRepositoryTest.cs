using System;
using System.Collections.Generic;
using System.IO;
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

        [Fact]
        public void Test01_Initialize()
        {            
            var tableScript = @"CREATE TABLE [sample_entity](
	                                         [doc_number] [int] PRIMARY KEY NOT NULL,
	                                         [creation_date] [datetime] NOT NULL,
	                                         [name] [varchar](200) NOT NULL,
	                                         [age] [int] NULL,
	                                         [height] [decimal](18, 2) NULL,
	                                         [weight] [decimal](18, 2) NULL,
	                                         [active] [bit] NOT NULL)";
            
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(tableScript, databaseFileName);
            }

            Assert.True(File.Exists(databaseFileName));
        }

        [Fact]
        public void Test02_Create()
        {
            int result;
            var sampleEntity = new SampleEntity() {
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

            var key = 12345;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.GetSync(key);
            }

            Assert.NotNull(result);
            Assert.Equal(key, result.DocNumber);
        }

        [Fact]
        public void Test04_GetByFilter()
        {
            SampleEntity result;

            var filter = new SampleEntity() { Name = "Roberto Torres" };
            using(var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.GetSync(filter);
            }

            Assert.NotNull(result);
            Assert.Equal(filter.Name, result.Name);
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
        public void Test07_Search()
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
        public void Test08_Count()
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
        public void Test09_Edit()
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
        public void Test10_Delete()
        {
            int result = 0;
            var filter = new SampleEntity() { DocNumber = 12345 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.DeleteSync(filter);
            }

            Assert.True(result > 0);
        }
    }
}
