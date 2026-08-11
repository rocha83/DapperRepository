using System;
using Rochas.DapperRepository.Specification.Enums;

namespace Rochas.DapperRepository.Helpers
{
    /// <summary>
    /// Detects the database engine from a connection string, when it was not explicitly supplied.
    /// Uses provider-specific markers to disambiguate between the supported engines.
    /// </summary>
    public static class DatabaseEngineDetector
    {
        /// <summary>
        /// Infers the <see cref="DatabaseEngine"/> from a connection string.
        /// Falls back to <see cref="DatabaseEngine.SQLite"/> when no marker matches.
        /// </summary>
        public static DatabaseEngine Detect(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                return DatabaseEngine.SQLite;

            var cs = connectionString.Trim().ToLowerInvariant();

            // URL schemes are unambiguous.
            if (cs.StartsWith("postgres://") || cs.StartsWith("postgresql://") || cs.StartsWith("pgsql://"))
                return DatabaseEngine.PostgreSQL;

            if (cs.StartsWith("mysql://") || cs.StartsWith("mariadb://"))
                return DatabaseEngine.MySQL;

            if (cs.StartsWith("sqlserver://") || cs.StartsWith("mssql://"))
                return DatabaseEngine.SQLServer;

            // Npgsql canonical key. Avoid "Host=" in favor of explicit markers below.
            if (cs.Contains("host="))
                return DatabaseEngine.PostgreSQL;

            if (cs.Contains("port=5432"))
                return DatabaseEngine.PostgreSQL;

            if (cs.Contains("port=3306"))
                return DatabaseEngine.MySQL;

            // Microsoft.Data.SqlClient specific markers (checked before the generic
            // "Server=" fallback so SQL Server is not misread as MySQL).
            if (cs.Contains("initial catalog=")
                || cs.Contains("integrated security=")
                || cs.Contains("trustservercertificate")
                || cs.Contains("multipleactiveresultsets")
                || (cs.Contains("encrypt=") && !cs.Contains("ssl mode=") && !cs.Contains("sslmode=")))
                return DatabaseEngine.SQLServer;

            // MySqlConnector canonical shape: "Server=" + user credential, without
            // SQL Server markers. SQL Server uses "User Id=" too, but it was already
            // ruled out by the markers above (Initial Catalog / Encrypt, etc).
            if ((cs.Contains("user=") || cs.Contains("user id="))
                && cs.Contains("server=")
                && !cs.Contains("data source="))
                return DatabaseEngine.MySQL;

            // A bare "Server=" (without Npgsql "Host=") is a SQL Server form.
            if (cs.Contains("server=") && !cs.Contains("host="))
                return DatabaseEngine.SQLServer;

            // "Data Source=" with a file path is SQLite (Microsoft.Data.SqlClient
            // would pair it with SQL Server markers already handled above).
            if (cs.Contains("data source="))
                return DatabaseEngine.SQLite;

            return DatabaseEngine.SQLite;
        }
    }
}
