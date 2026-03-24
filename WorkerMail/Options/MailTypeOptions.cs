using System.ComponentModel.DataAnnotations;

namespace WorkerMail.Options;

public sealed class MailTypeOptions
{
    public const string SectionName = "MailTypes";

    [Required]
    public string DefaultSenderProfile { get; set; } = null!;

    [Required]
    public Dictionary<string, MailTypeDefinitionOptions> Definitions { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}
