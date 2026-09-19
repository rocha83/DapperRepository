using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using Npgsql;
using Rochas.DapperRepository.Base;
using Xunit;

namespace Rochas.DapperRepository.Test
{
    public class PostgreSqlCompositeCommandTest
    {
        private const string DummyConnString = "Host=localhost;Username=test;Password=test;Database=test";

        private static IDbCommand InvokeComposite(DatabaseConnection dbConnection, Dictionary<object, object> parameters)
        {
            var compositeCommand = typeof(DatabaseConnection).GetMethod(
                "CompositeCommand", BindingFlags.NonPublic | BindingFlags.Instance);

            Assert.NotNull(compositeCommand);

            return (IDbCommand)compositeCommand.Invoke(
                dbConnection,
                new object[] { "SELECT COUNT(*) FROM sample WHERE name = @name", parameters });
        }

        [Fact]
        public void Test001_PgComposite_ProducesNpgsqlParameters()
        {
            using var dbConnection = new DatabaseConnection(new NpgsqlConnection(DummyConnString));

            var command = InvokeComposite(dbConnection, new Dictionary<object, object>
            {
                { "@name", "Maria" },
                { "@age", 30 }
            });

            Assert.Equal(2, command.Parameters.Count);
            foreach (IDataParameter parameter in command.Parameters)
                Assert.IsType<NpgsqlParameter>(parameter);
        }

        [Fact]
        public void Test002_PgComposite_MapsNullToDbNull()
        {
            using var dbConnection = new DatabaseConnection(new NpgsqlConnection(DummyConnString));

            var command = InvokeComposite(dbConnection, new Dictionary<object, object>
            {
                { "@resume", null }
            });

            var parameter = (IDataParameter)command.Parameters[0];
            Assert.NotNull(parameter);
            Assert.Equal(DBNull.Value, parameter.Value);
        }

        [Fact]
        public void Test003_PgComposite_MapsGuidToText()
        {
            var id = Guid.NewGuid();

            using var dbConnection = new DatabaseConnection(new NpgsqlConnection(DummyConnString));

            var command = InvokeComposite(dbConnection, new Dictionary<object, object>
            {
                { "@id", id }
            });

            var parameter = (IDataParameter)command.Parameters[0];
            Assert.Equal(id.ToString("D"), parameter.Value);
        }

        [Fact]
        public void Test004_PgComposite_KeepsScalarValues()
        {
            var creationDate = new DateTime(2026, 9, 17, 12, 0, 0, DateTimeKind.Utc);

            using var dbConnection = new DatabaseConnection(new NpgsqlConnection(DummyConnString));

            var command = InvokeComposite(dbConnection, new Dictionary<object, object>
            {
                { "@name", "Maria" },
                { "@age", 30 },
                { "@active", true },
                { "@creation_date", creationDate }
            });

            Assert.Equal(4, command.Parameters.Count);
            Assert.Equal("Maria", ((IDataParameter)command.Parameters[0]).Value);
            Assert.Equal(30, ((IDataParameter)command.Parameters[1]).Value);
            Assert.Equal(true, ((IDataParameter)command.Parameters[2]).Value);
            Assert.Equal(creationDate, ((IDataParameter)command.Parameters[3]).Value);
        }
    }
}
