using System;
using System.Collections;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;

namespace Rochas.DapperRepository.Helpers
{
    /// <summary>
    /// Converte colunas TEXT (CSV) para arrays 1-D de primitivos (uint[], int[],
    /// string[] etc.) na leitura via Dapper. Complementa a serialização feita em
    /// <see cref="EntityReflector.FormatSQLInputValue"/> na persistência.
    /// </summary>
    public sealed class PrimitiveArrayTypeHandler : SqlMapper.ITypeHandler
    {
        private static bool _registered;

        public static void EnsureRegistered()
        {
            if (_registered) return;
            _registered = true;

            var supportedElementTypes = new[]
            {
                typeof(uint), typeof(int), typeof(long), typeof(short),
                typeof(ushort), typeof(ulong), typeof(float), typeof(double),
                typeof(decimal), typeof(bool), typeof(string)
            };

            foreach (var elementType in supportedElementTypes)
                SqlMapper.AddTypeHandler(elementType.MakeArrayType(), new PrimitiveArrayTypeHandler());
        }

        public object Parse(Type destinationType, object value)
        {
            if (value == null || value == DBNull.Value)
                return Array.CreateInstance(destinationType.GetElementType(), 0);

            var elementType = destinationType.GetElementType();
            var text = Convert.ToString(value, CultureInfo.InvariantCulture);
            if (string.IsNullOrWhiteSpace(text))
                return Array.CreateInstance(elementType, 0);

            var parts = text.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            var result = Array.CreateInstance(elementType, parts.Length);
            for (int i = 0; i < parts.Length; i++)
                result.SetValue(Convert.ChangeType(parts[i].Trim(), elementType, CultureInfo.InvariantCulture), i);
            return result;
        }

        public void SetValue(IDbDataParameter parameter, object value)
        {
            if (value == null)
            {
                parameter.Value = DBNull.Value;
                return;
            }

            var csv = string.Join(",", ((IEnumerable)value)
                .Cast<object>()
                .Select(v => Convert.ToString(v, CultureInfo.InvariantCulture)));
            parameter.Value = csv;
        }
    }
}
