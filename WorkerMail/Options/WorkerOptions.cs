using System.ComponentModel.DataAnnotations;

namespace WorkerMail.Options;

public sealed class WorkerOptions
{
    public const string SectionName = "Worker";

    [Required]
    [Range(100, 60000)]
    public int? PollIntervalMs { get; set; }

    [Required]
    [Range(100, 60000)]
    public int? IdleDelayMs { get; set; }

    [Required]
    [Range(100, 60000)]
    public int? RetryDelayMs { get; set; }

    [Required]
    [Range(1, 500)]
    public int? MaxConcurrentMessages { get; set; }

    [Required]
    [Range(1, 300)]
    public int? ShutdownDrainTimeoutSeconds { get; set; }

    [Required]
    [Range(5, 3600)]
    public int? LockTimeoutSeconds { get; set; }

    [Required]
    [Range(1, 100)]
    public int? MaxProcessingAttempts { get; set; }

    [Required]
    [Range(1, 720)]
    public int? AttemptKeyTtlHours { get; set; }

    [Required]
    [Range(1, 8760)]
    public int? ProcessedKeyTtlHours { get; set; }

    [Required]
    [Range(1, 1440)]
    public int? LastEventCacheTtlMinutes { get; set; }

    [Required]
    public bool? CacheLastEvent { get; set; }
}
