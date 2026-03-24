using System.ComponentModel.DataAnnotations;

namespace WorkerLogs.Options;

public sealed class WorkerOptions
{
    public const string SectionName = "Worker";

    [Required]
    [Range(1, 50000)]
    public int? BatchSize { get; set; }

    [Required]
    [Range(100, 60000)]
    public int? FlushIntervalMs { get; set; }

    [Required]
    [Range(100, 60000)]
    public int? RetryDelayMs { get; set; }
}
