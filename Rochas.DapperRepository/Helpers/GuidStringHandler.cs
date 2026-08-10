using System;
using System.Data;
using System.Globalization;
using Dapper;

namespace Rochas.DapperRepository.Helpers
{
    public sealed class GuidStringHandler : SqlMapper.TypeHandler<Guid>
    {
        private static bool _registered;

        public static void EnsureRegistered()
        {
            if (_registered) return;
            _registered = true;
            SqlMapper.RemoveTypeMap(typeof(Guid));
            SqlMapper.AddTypeHandler(new GuidStringHandler());
        }

        public override void SetValue(IDbDataParameter parameter, Guid value)
            => parameter.Value = value.ToString("D");

        public override Guid Parse(object value)
            => Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)!);
    }
}
