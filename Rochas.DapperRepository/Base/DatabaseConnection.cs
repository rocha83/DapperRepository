using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using System.Threading.Tasks;
using Dapper;
using MySqlConnector;
using Rochas.DapperRepository.Exceptions;
using Rochas.DapperRepository.Helpers;
using Rochas.SqlWrapper.Helpers;
using Rochas.SqlWrapper.Helpers.SQL;

using Rochas.Data.Specification.Enums;
using Npgsql;

namespace Rochas.DapperRepository.Base
{
    public class DatabaseConnection : DatabaseSettings, IDisposable
    {
        #region Declarations

        private readonly string insertCommand = SQLStatements.SQL_ReservedWord_INSERT;
        private readonly string countCommand = SQLStatements.SQL_ReservedWord_COUNT;

        private static void DumpSql(string sql)
        {
#if DEBUG
            try
            {
                var path = System.IO.Path.Combine(
                    System.IO.Path.GetTempPath(), "orm_sql_dump.log");
                System.IO.File.AppendAllText(path,
                    $"[{DateTime.Now:HH:mm:ss}] {sql}\n\n");
            }
            catch { }
#endif
        }

        protected bool keepConnection = false;
        protected DatabaseEngine engine;
        protected IDbConnection connection;
        protected IDbTransaction transactionControl;

        #endregion

        #region Constructors

        public DatabaseConnection(DatabaseEngine databaseEngine, string connectionString, string logPath = null, bool keepConnected = false, params string[] replicaConnStrings) : base(connectionString, logPath, replicaConnStrings)
        {
            engine = databaseEngine;

            keepConnection = keepConnected;
            if (keepConnection) Connect();
        }

        /// <summary>
        /// Infers the <see cref="DatabaseEngine"/> from the connection string.
        /// </summary>
        public DatabaseConnection(string connectionString, string logPath = null, bool keepConnected = false, params string[] replicaConnStrings)
            : this(DatabaseEngineDetector.Detect(connectionString), connectionString, logPath, keepConnected, replicaConnStrings)
        {
        }

        public DatabaseConnection(IDbConnection dbConnection, string logPath = null, bool keepConnected = false, params string[] replicaConnStrings) : base(dbConnection.ConnectionString, logPath, replicaConnStrings)
        {
            engine = dbConnection switch
            {
                Microsoft.Data.Sqlite.SqliteConnection => DatabaseEngine.SQLite,
                MySqlConnector.MySqlConnection => DatabaseEngine.MySQL,
                Microsoft.Data.SqlClient.SqlConnection => DatabaseEngine.SQLServer,
                Npgsql.NpgsqlConnection => DatabaseEngine.PostgreSQL,
                _ => DatabaseEngineDetector.Detect(dbConnection.ConnectionString)
            };
            connection = dbConnection;

            keepConnection = keepConnected;
            if (keepConnection) Connect();
        }

        #endregion

        #region Public Methods

        public void StartTransaction()
        {
            if ((connection == null)
                || (connection.State != ConnectionState.Open))
                keepConnection = Connect();

            if (transactionControl != null)
                return;

            this.transactionControl = connection.BeginTransaction();
        }

        public void CommitTransaction()
        {
            if ((connection != null) && (connection.State == ConnectionState.Open)
                && (transactionControl != null))
            {
                transactionControl.Commit();
                transactionControl.Dispose();
                transactionControl = null;
                keepConnection = false;
            }
        }

        public void CancelTransaction()
        {
            if ((connection != null) && (connection.State == ConnectionState.Open)
                && (transactionControl != null))
            {
                transactionControl.Rollback();
                transactionControl.Dispose();
                transactionControl = null;
                keepConnection = false;
            }
        }

        public void Dispose()
        {
            if (transactionControl != null)
            {
                try { transactionControl.Rollback(); } catch { }
                try { transactionControl.Dispose(); } catch { }
            }

            if (connection != null)
                try { connection.Dispose(); } catch { }

            GC.ReRegisterForFinalize(this);
        }

        #endregion

        #region Helper Methods

        protected bool Connect(string optionalConnConfig = "")
        {
            if (!string.IsNullOrEmpty(_connString) || !string.IsNullOrEmpty(optionalConnConfig))
            {
                if (connection == null)
                {
                    PrimitiveArrayTypeHandler.EnsureRegistered();
                    ByteArrayBase64Handler.EnsureRegistered();
                    switch (engine)
                    {
                        case DatabaseEngine.MySQL:
                            connection = new MySqlConnection();
                            break;
                        case DatabaseEngine.SQLServer:
                            connection = new SqlConnection();
                            break;
                        case DatabaseEngine.PostgreSQL:
                            connection = new NpgsqlConnection();
                            break;
                        case DatabaseEngine.SQLite:
                            connection = new SqliteConnection();
                            GuidStringHandler.EnsureRegistered();
                            break;
                    }
                }

                if ((connection.State != ConnectionState.Open) && (connection.State != ConnectionState.Connecting))
                {
                    if (!string.IsNullOrEmpty(optionalConnConfig))
                        connection.ConnectionString = optionalConnConfig;
                    else
                        connection.ConnectionString = _connString;

                    connection.Open();
                }
            }
            else
                throw new ConnectionStringNotFoundException();

            return (connection.State == ConnectionState.Open);
        }

        protected bool Disconnect()
        {
            if ((connection != null) && (connection.State == ConnectionState.Open))
            {
                connection.Close();
                connection.Dispose();
                connection = null;
            }

            return true;
        }

        protected IEnumerable<object> ExecuteQuery(Type entityType, string sqlInstruction, Dictionary<string, object> parameters = null)
        {
            IEnumerable<object> result;

            if (connection.State != ConnectionState.Open)
                Connect();

            result = connection.Query(entityType, sqlInstruction, parameters);

            return result;
        }

        protected async Task<IEnumerable<object>> ExecuteQueryAsync(Type entityType, string sqlInstruction, Dictionary<string, object> parameters = null)
        {
            IEnumerable<object> result;

            if (connection.State != ConnectionState.Open)
                Connect();

            DumpSql(sqlInstruction);

            result = await connection.QueryAsync(entityType, sqlInstruction, parameters);

            return result;
        }

        protected async Task<int> ExecuteCountAsync(string sqlInstruction, Dictionary<string, object> parameters = null)
        {
            if (connection.State != ConnectionState.Open)
                Connect();

            var result = await connection.QuerySingleOrDefaultAsync<int>(sqlInstruction, parameters);
            return result;
        }

        private string GetLastIdSql()
        {
            switch (engine)
            {
                case DatabaseEngine.SQLite:
                    return SQLStatements.SQL_Action_GetLastId_SQLite;
                case DatabaseEngine.PostgreSQL:
                    return SQLStatements.SQL_Action_GetLastId_PostgreSQL;
                case DatabaseEngine.MySQL:
                    return SQLStatements.SQL_Action_GetLastId_MySQL;
                default:
                    return SQLStatements.SQL_Action_GetLastId;
            }
        }

        protected int ExecuteCommand(string sqlInstruction, Dictionary<object, object> parameters = null)
        {
            IDbCommand sqlCommand;

            int executionReturn = 0;

            DumpSql(sqlInstruction);

            if (connection.State == ConnectionState.Open)
            {
                sqlCommand = CompositeCommand(sqlInstruction, parameters);

                if (sqlCommand.CommandText.StartsWith(insertCommand)
                    || sqlCommand.CommandText.Contains(countCommand))
                {
                    if (sqlCommand.CommandText.StartsWith(insertCommand))
                    {
                        sqlCommand.ExecuteNonQuery();
                        sqlCommand.CommandText = GetLastIdSql();
                    }

                    int.TryParse(sqlCommand.ExecuteScalar().ToString(), out int scalarReturn);
                    executionReturn = scalarReturn;
                }
                else
                    executionReturn = sqlCommand.ExecuteNonQuery();
            }

            return executionReturn;
        }

        protected async Task<int> ExecuteCommandAsync(string sqlInstruction, Dictionary<object, object> parameters = null)
        {
            IDbCommand sqlCommand;

            int executionReturn = 0;

            DumpSql(sqlInstruction);

            if (connection.State == ConnectionState.Open)
            {
                sqlCommand = CompositeCommand(sqlInstruction, parameters);

                if (sqlCommand.CommandText.StartsWith(insertCommand)
                    || sqlCommand.CommandText.Contains(countCommand))
                {
                    if (sqlCommand.CommandText.StartsWith(insertCommand))
                    {
                        sqlCommand.ExecuteNonQuery();
                        sqlCommand.CommandText = GetLastIdSql();
                    }

                    int.TryParse(sqlCommand.ExecuteScalar().ToString(), out int scalarReturn);
                    executionReturn = scalarReturn;
                }
                else
                    executionReturn = await connection.ExecuteAsync(sqlInstruction, transaction: transactionControl);
            }

            return executionReturn;
        }

        protected void ExecuteBulkCommand(DataTable entitiesTable)
        {
            if (connection.State != ConnectionState.Open)
                Connect();

            using var bulkCmd = new SqlBulkCopy(_connString);
            bulkCmd.WriteToServer(entitiesTable);
        }

        protected async Task ExecuteBulkCommandAsync(DataTable entitiesTable)
        {
            if (connection.State != ConnectionState.Open)
                Connect();

            using var bulkCmd = new SqlBulkCopy(_connString);
            await bulkCmd.WriteToServerAsync(entitiesTable);
        }

        private IDbCommand CompositeCommand(string sqlInstruction, Dictionary<object, object> parameters = null)
        {
            var sqlCommand = connection.CreateCommand();
            sqlCommand.CommandText = sqlInstruction;

            if ((transactionControl != null)
                    && (transactionControl.Connection != null))
                sqlCommand.Transaction = transactionControl;

            if (parameters != null)
            {
                sqlCommand.Parameters.Clear();

                foreach (var param in parameters)
                {
                    IDataParameter newSqlParameter = null;

                    switch (engine)
                    {
                        case DatabaseEngine.MySQL:
                            newSqlParameter = new MySqlParameter(param.Key.ToString(), param.Value);
                            break;
                        case DatabaseEngine.SQLServer:
                            newSqlParameter = new SqlParameter(param.Key.ToString(), param.Value);
                            break;
                        case DatabaseEngine.SQLite:
                            newSqlParameter = new SqliteParameter(param.Key.ToString(), param.Value);
                            break;
                    }

                    sqlCommand.Parameters.Add(newSqlParameter);
                }
            }

            if (transactionControl != null)
                sqlCommand.Transaction = transactionControl;

            return sqlCommand;
        }

        #endregion
    }
}
