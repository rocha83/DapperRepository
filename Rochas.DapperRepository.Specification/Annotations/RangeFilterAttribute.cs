using System;
using System.Collections.Generic;
using System.Text;

namespace Rochas.Data.Specification.Annotations
{
    public class RangeFilterAttribute : Attribute
    {
        public string LinkedRangeProperty { get; set; }
    }
}
