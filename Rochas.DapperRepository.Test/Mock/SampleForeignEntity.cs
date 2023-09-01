using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Rochas.DapperRepository.Specification.Annotations;

namespace Rochas.DapperRepository.Test
{
    [Table("sample_foreign_entity")]
    public class SampleForeignEntity
    {
        [Key]

        [Column("parent_id")]
        public int ParentId { get; set; }

        [Filterable]
        [Column("title")]
        public string Title { get; set; }

        [Filterable]
        [Column("description")]
        public string Description { get; set; }
    }
}
