using Xunit;
using Rochas.DapperRepository.Helpers;
using Rochas.DapperRepository.Specification.Enums;

namespace Rochas.DapperRepository.Test
{
    public class DatabaseEngineDetectorTests
    {
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void Detect_NullOrEmpty_ReturnsSQLite(string connString)
        {
            Assert.Equal(DatabaseEngine.SQLite, DatabaseEngineDetector.Detect(connString));
        }

        [Theory]
        [InlineData("postgres://user:pass@localhost/db")]
        [InlineData("postgresql://user:pass@localhost/db")]
        [InlineData("pgsql://user:pass@localhost/db")]
        public void Detect_PostgreUrlSchemes(string connString)
        {
            Assert.Equal(DatabaseEngine.PostgreSQL, DatabaseEngineDetector.Detect(connString));
        }

        [Theory]
        [InlineData("mysql://user:pass@localhost/db")]
        [InlineData("mariadb://user:pass@localhost/db")]
        public void Detect_MySqlUrlSchemes(string connString)
        {
            Assert.Equal(DatabaseEngine.MySQL, DatabaseEngineDetector.Detect(connString));
        }

        [Theory]
        [InlineData("sqlserver://user:pass@localhost")]
        [InlineData("mssql://user:pass@localhost")]
        public void Detect_SqlServerUrlSchemes(string connString)
        {
            Assert.Equal(DatabaseEngine.SQLServer, DatabaseEngineDetector.Detect(connString));
        }

        [Fact]
        public void Detect_Host_Marker_ReturnsPostgre()
        {
            var connString = "Host=localhost;Database=db;Username=user";
            Assert.Equal(DatabaseEngine.PostgreSQL, DatabaseEngineDetector.Detect(connString));
        }

        [Fact]
        public void Detect_Port_5432_ReturnsPostgre()
        {
            var connString = "Server=localhost;Port=5432;Database=db";
            Assert.Equal(DatabaseEngine.PostgreSQL, DatabaseEngineDetector.Detect(connString));
        }

        [Fact]
        public void Detect_Port_3306_ReturnsMySql()
        {
            var connString = "Server=localhost;Port=3306;Database=db";
            Assert.Equal(DatabaseEngine.MySQL, DatabaseEngineDetector.Detect(connString));
        }

        [Theory]
        [InlineData("Server=.;Database=db;Initial Catalog=test")]
        [InlineData("Server=.;Integrated Security=True")]
        [InlineData("Server=.;TrustServerCertificate=True")]
        [InlineData("Server=.;MultipleActiveResultSets=True")]
        [InlineData("Server=.;Encrypt=True;Database=db")]
        public void Detect_SqlServerMarkers(string connString)
        {
            Assert.Equal(DatabaseEngine.SQLServer, DatabaseEngineDetector.Detect(connString));
        }

        [Fact]
        public void Detect_MySqlShape()
        {
            var connString = "Server=localhost;User=root;Password=pwd;Database=db";
            Assert.Equal(DatabaseEngine.MySQL, DatabaseEngineDetector.Detect(connString));
        }

        [Fact]
        public void Detect_Server_WithoutHost_ReturnsSqlServer()
        {
            var connString = "Server=localhost;Database=db";
            Assert.Equal(DatabaseEngine.SQLServer, DatabaseEngineDetector.Detect(connString));
        }

        [Fact]
        public void Detect_UserIdAndServer_ReturnsMySql()
        {
            var connString = "Server=localhost;Database=db;User Id=sa";
            Assert.Equal(DatabaseEngine.MySQL, DatabaseEngineDetector.Detect(connString));
        }

        [Fact]
        public void Detect_Data_Source_ReturnsSQLite()
        {
            var connString = "Data Source=sample.db;Cache=Shared";
            Assert.Equal(DatabaseEngine.SQLite, DatabaseEngineDetector.Detect(connString));
        }

        [Fact]
        public void Detect_Fallback_ReturnsSQLite()
        {
            var connString = "Foo=Bar;Baz=Qux";
            Assert.Equal(DatabaseEngine.SQLite, DatabaseEngineDetector.Detect(connString));
        }

        [Fact]
        public void Detect_UserAndServer_WithDataSource_NotMySql()
        {
            var connString = "Data Source=server;User=root;Password=pwd";
            Assert.Equal(DatabaseEngine.SQLite, DatabaseEngineDetector.Detect(connString));
        }

        [Fact]
        public void Detect_Encrypt_WithSslMode_NotSqlServer()
        {
            var connString = "Host=localhost;Database=db;SslMode=Require;Encrypt=True";
            Assert.Equal(DatabaseEngine.PostgreSQL, DatabaseEngineDetector.Detect(connString));
        }
    }
}