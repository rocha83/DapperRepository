using System;
using System.ComponentModel.DataAnnotations.Schema;
using Rochas.DapperRepository.Specification.Annotations;

namespace Rochas.DapperRepository.Test
{    
    public abstract class SampleForeignEntity
    {
        [Filterable]
        [Column("title")]
        public string Title { get; set; }

        [Filterable]
        [Column("description")]
        public string Description { get; set; }
    }
}
