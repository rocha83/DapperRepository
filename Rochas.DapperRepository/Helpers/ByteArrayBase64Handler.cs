using System;
using System.Data;
using Dapper;

namespace Rochas.DapperRepository.Helpers
{
    /// <summary>
    /// Converte colunas TEXT (base64) para byte[] na leitura via Dapper.
    /// Complementa a serialização feita em <see cref="EntityReflector.FormatSQLInputValue"/>
    /// na persistência: byte[] é salvo como base64 em coluna TEXT (bancos limitados
    /// sem blob nativo no mapeamento da ORM).
    /// </summary>
    public sealed class ByteArrayBase64Handler : SqlMapper.TypeHandler<byte[]>
    {
        private static bool _registered;

        public static void EnsureRegistered()
        {
            if (_registered) return;
            _registered = true;
            SqlMapper.RemoveTypeMap(typeof(byte[]));
            SqlMapper.AddTypeHandler(typeof(byte[]), new ByteArrayBase64Handler());
        }

        public override void SetValue(IDbDataParameter parameter, byte[] value)
        {
            parameter.Value = (value == null) ? DBNull.Value : Convert.ToBase64String(value);
        }

        public override byte[] Parse(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;

            if (value is byte[] rawBytes)
                return rawBytes;

            var text = Convert.ToString(value);
            return string.IsNullOrWhiteSpace(text) ? null : Convert.FromBase64String(text);
        }
    }
}
