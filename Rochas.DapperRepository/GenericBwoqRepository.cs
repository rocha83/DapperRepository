using System.Data;
using System.Threading.Tasks;
using Rochas.Data.Specification.Enums;
using Rochas.Data.Specification.Interfaces;

namespace Rochas.DapperRepository
{
	/// <summary>
	/// Repository reduced to persistence + read + BWOQ grammar (Q/W/G/O/OD).
	/// Inherits PersistenceRepository for write/read, uses internal GenericRepository
	/// only for BWOQ query execution.
	/// </summary>
	public class GenericBwoqRepository<T> : PersistenceRepository<T>, IGenericBwoqRepository<T> where T : class
	{
		private readonly GenericRepository<T> _bwoqExec;

		public GenericBwoqRepository(DatabaseEngine engine, string connectionString, string logPath = null,
			bool keepConnected = false, bool readUncommied = false, bool forceSnakeCase = false,
			ICacheProvider cacheProvider = null, params string[] replicaConnStrings)
			: base(engine, connectionString, logPath, keepConnected, readUncommied, forceSnakeCase, cacheProvider, replicaConnStrings)
		{
			_bwoqExec = new GenericRepository<T>(engine, connectionString, logPath, keepConnected,
				readUncommied, forceSnakeCase, cacheProvider, replicaConnStrings);
		}

		public GenericBwoqRepository(string connectionString, string logPath = null,
			bool keepConnected = false, bool readUncommied = false, bool forceSnakeCase = false,
			ICacheProvider cacheProvider = null, params string[] replicaConnStrings)
			: base(connectionString, logPath, keepConnected, readUncommied, forceSnakeCase, cacheProvider, replicaConnStrings)
		{
			_bwoqExec = new GenericRepository<T>(connectionString, logPath, keepConnected,
				readUncommied, forceSnakeCase, cacheProvider, replicaConnStrings);
		}

		public GenericBwoqRepository(IDbConnection dbConnection, string logPath = null,
			bool keepConnected = false, bool readUncommied = false, bool forceSnakeCase = false,
			ICacheProvider cacheProvider = null, params string[] replicaConnStrings)
			: base(dbConnection, logPath, keepConnected, readUncommied, forceSnakeCase, cacheProvider, replicaConnStrings)
		{
			_bwoqExec = new GenericRepository<T>(dbConnection, logPath, keepConnected,
				readUncommied, forceSnakeCase, cacheProvider, replicaConnStrings);
		}

		// ── BWOQ QUERY ───────────────────────────────────────────────

		public IBwoqQueryBuilder<T> QueryBwoq()
			=> _bwoqExec.QueryBwoq();
	}
}
