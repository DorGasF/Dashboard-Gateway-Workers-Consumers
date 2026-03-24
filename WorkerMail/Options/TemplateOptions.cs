using System.ComponentModel.DataAnnotations;

namespace WorkerMail.Options;

public sealed class TemplateOptions
{
    public const string SectionName = "Templates";

    [Required]
    public string BasePath { get; set; } = null!;
}
