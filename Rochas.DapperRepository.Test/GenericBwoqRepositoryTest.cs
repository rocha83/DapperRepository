using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Rochas.Data.Specification.Enums;
using Rochas.Data.Specification.Interfaces;

namespace Rochas.DapperRepository.Test
{
    public class GenericBwoqRepositoryTest
    {
        private static string databaseFileName = "MockBwoq.sqlite";
        private static string connString = "Data Source=MockBwoq.sqlite;Mode=ReadWriteCreate;";

        static GenericBwoqRepositoryTest()
        {
            var tableScript = @"CREATE TABLE IF NOT EXISTS [sample_entity](
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

            using (var repos = new GenericBwoqRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(tableScript);
            }
        }

        [Fact]
        public void Test001_BwoqRepo_CanInstantiate()
        {
            using var repos = new GenericBwoqRepository<SampleEntity>(DatabaseEngine.SQLite, connString);
            Assert.NotNull(repos);
        }

        [Fact]
        public void Test002_BwoqRepo_AddAndRemove()
        {
            using var repos = new GenericBwoqRepository<SampleEntity>(DatabaseEngine.SQLite, connString);

            var entity = new SampleEntity
            {
                DocNumber = 999001,
                CreationDate = DateTime.Now,
                Name = "BWOQ Test Entity",
                Age = 25,
                Active = true
            };

            var addResult = repos.AddSync(entity);
            Assert.True(addResult > 0);

            var retrieved = repos.GetSync(entity);
            Assert.NotNull(retrieved);
            Assert.Equal("BWOQ Test Entity", retrieved.Name);

            var removeResult = repos.RemoveSync(retrieved);
            Assert.True(removeResult > 0);
        }

        [Fact]
        public void Test003_BwoqRepo_QueryBwoq_Where()
        {
            using var repos = new GenericBwoqRepository<SampleEntity>(DatabaseEngine.SQLite, connString);

            var entity = new SampleEntity
            {
                DocNumber = 999002,
                CreationDate = DateTime.Now,
                Name = "BWOQ Where Test",
                Age = 30,
                Active = true
            };
            repos.AddSync(entity);

            // BWOQ syntax: columnIndex::value (Name = column 32)
            var results = repos.QueryBwoq()
                .W("32::BWOQ Where Test")
                .ToQuerySync()
                .ToList();

            Assert.NotEmpty(results);
            Assert.Contains(results, r => r.Name == "BWOQ Where Test");

            repos.RemoveSync(entity);
        }

        [Fact]
        public void Test004_BwoqRepo_QueryBwoq_WhereAndOrder()
        {
            using var repos = new GenericBwoqRepository<SampleEntity>(DatabaseEngine.SQLite, connString);

            var entity1 = new SampleEntity
            {
                DocNumber = 999003,
                CreationDate = DateTime.Now,
                Name = "BWOQ Alpha",
                Age = 20,
                Active = true
            };
            var entity2 = new SampleEntity
            {
                DocNumber = 999004,
                CreationDate = DateTime.Now,
                Name = "BWOQ Beta",
                Age = 35,
                Active = true
            };
            repos.AddSync(entity1);
            repos.AddSync(entity2);

            // W: filter by Active (column 1024) = true, O: order by Name (column 32)
            var results = repos.QueryBwoq()
                .W("1024::true")
                .O("32")
                .ToQuerySync()
                .ToList();

            Assert.True(results.Count >= 2);
            var names = results.Select(r => r.Name).ToList();
            var alphaIdx = names.IndexOf("BWOQ Alpha");
            var betaIdx = names.IndexOf("BWOQ Beta");
            Assert.True(alphaIdx < betaIdx);

            repos.RemoveSync(entity1);
            repos.RemoveSync(entity2);
        }

        [Fact]
        public void Test005_BwoqRepo_ImplementsIGenericBwoqRepository()
        {
            using var repos = new GenericBwoqRepository<SampleEntity>(DatabaseEngine.SQLite, connString);

            IGenericBwoqRepository<SampleEntity> bwoqRepos = repos;
            Assert.NotNull(bwoqRepos);
            Assert.NotNull(bwoqRepos.QueryBwoq());
        }
    }
}
