using System.ComponentModel.DataAnnotations;

namespace WorkerLogs.Options;

public sealed class ConsoleLoggingOptions
{
    public const string SectionName = "ConsoleLogging";

    [Required]
    public string TimestampFormat { get; set; } = null!;

    [Required]
    public bool? SingleLine { get; set; }
}
