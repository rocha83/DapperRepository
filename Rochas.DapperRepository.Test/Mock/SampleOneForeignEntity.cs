using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Rochas.DapperRepository.Specification.Annotations;

namespace Rochas.DapperRepository.Test
{
    [Table("sample_one_foreign_entity")]
    public class SampleOneForeignEntity : SampleForeignEntity
    {
        [Key]
        [Column("parent_id")]
        public int ParentId { get; set; }
    }
}
