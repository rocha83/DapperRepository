﻿using System;
using System.Collections.Generic;
using System.Text;

namespace Rochas.DapperRepository.Specification.Annotations
{
    public class RangeFilterAttribute : Attribute
    {
        public string LinkedRangeProperty { get; set; }
    }
}
