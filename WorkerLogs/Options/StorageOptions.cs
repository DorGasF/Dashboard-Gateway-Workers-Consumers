using System.ComponentModel.DataAnnotations;

namespace WorkerLogs.Options;

public sealed class StorageOptions
{
    public const string SectionName = "Storage";

    [Required]
    public string LogsDirectory { get; set; } = null!;
}
