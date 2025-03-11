using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Rochas.DapperRepository.Specification.Enums;
using Microsoft.VisualStudio.TestPlatform.Utilities;
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
        public void Test02_Create()
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
                result = repos.CreateSync(sampleEntity1);
            }
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result += repos.CreateSync(sampleEntity2);
            }

            Assert.True(result > 1);
        }

        [Fact]
        public void Test03_CreateComposition()
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
                result = repos.CreateSync(sampleEntity, true);
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
        public void Test06_List()
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
        public void Test07_ListLimited()
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
        public void Test08_ListSorted()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter,
                                        sortAttributes: "Name");
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
            Assert.StartsWith("Gustavo", result.First().Name);

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter,
                                        sortAttributes: "Name",
                                        orderDescending: true);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
            Assert.StartsWith("Roberto", result.First().Name);
        }

        [Fact]
        public void Test09_ListByDateRange()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity()
            {
                CreationDate = DateTime.Now.Date.AddDays(-1),
                CreationDateEnd = DateTime.Now.Date.AddDays(1)
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter, filterConjunction: false);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test10_ListByAgeMajorThan()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity()
            {
                Age = 16
            };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter);
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
                var criterias = "roberto torres sao paulo".Tokenize();

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
        public void Test14_Edit()
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
        public void Test15_EditComposition()
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
                    result = repos.EditSync(item, filter, true);
                }
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test16_Delete()
        {
            int result = 0;
            var filter = new SampleEntity() { DocNumber = 12345 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.DeleteSync(filter);
            }

            filter = new SampleEntity() { DocNumber = 76910 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result += repos.DeleteSync(filter);
            }

            Assert.True(result > 1);
        }

        #endregion

        #region OneToOne Composite Entity Tests

        [Fact]
        public void Test17_OneToOneCompositionCreate()
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
                result = repos.CreateSync(sampleEntity, true);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test18_GetOneToOneCompositionByKey()
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
        public void Test19_ListOneToOneComposition()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "alberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter, true);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());

            Assert.NotNull(result.First().OneToOneForeignEntity);
        }

        #endregion

        #region ManyToOne Composition Entity Tests

        [Fact]
        public void Test20_ManyToOneCompositionCreate()
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
                result = repos.CreateSync(manyToOneForeignEntity, true);
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
                result = repos.CreateSync(sampleEntity, true);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test21_GetManyToOneCompositionByKey()
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
        public void Test22_ListManyToOneComposition()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "claudia" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.ListSync(filter, true);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());

            Assert.NotNull(result.First().ManyToOneForeignEntity);
        }

        #endregion

        #region OneToMany Composite Entity Tests

        [Fact]
        public void Test23_OneToManyCompositionCreate()
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
                result = repos.CreateSync(sampleEntity, true);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test24_GetOneToManyCompositionByKey()
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
        public void Test25_ListOneToManyComposition()
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
            Assert.NotNull(firstItem.OneToManyForeignEntities);
            Assert.True(firstItem.OneToManyForeignEntities.Count == 6);
        }

        #endregion

        #region ManyToMany Composite Entity Tests

        [Fact]
        public void Test26_IntermedyCompositionCreate()
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
            leftEntityResult = leftEntityRepos.CreateSync(sampleLeftEntity);

            using var rightEntityRepos = new GenericRepository<SampleManyForeignEntity>(DatabaseEngine.SQLite, connString);
            rightEntityResult = rightEntityRepos.CreateSync(sampleRightEntity);

            var sampleIntermedyEntity = new SampleIntermedyForeignEntity()
            {
                LeftSideId = leftEntityResult,
                RightSideId = rightEntityResult
            };

            using var repos = new GenericRepository<SampleIntermedyForeignEntity>(DatabaseEngine.SQLite, connString);
            var result = repos.CreateSync(sampleIntermedyEntity);

            Assert.True(result > 0);
        }

        [Fact]
        public void Test27_IntermedyCompositionGet()
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
    }
}
