using System.ComponentModel.DataAnnotations;

namespace WorkerMail.Options;

public sealed class RedisOptions
{
    public const string SectionName = "Redis";

    [Required]
    public string Host { get; set; } = null!;

    [Required]
    [Range(1, 65535)]
    public int? Port { get; set; }

    [Required]
    [Range(0, 1000)]
    public int? Database { get; set; }

    [Required]
    [Range(0, 1000)]
    public int? ConnectRetry { get; set; }

    [Required]
    public bool? AbortOnConnectFail { get; set; }

    [Required]
    [Range(1000, 60000)]
    public int? ReconnectRetryDelayMs { get; set; }

    [Required]
    [Range(1000, 60000)]
    public int? ConnectTimeoutMs { get; set; }

    [Required]
    [Range(1000, 60000)]
    public int? AsyncTimeoutMs { get; set; }

    [Required]
    [Range(1, 300)]
    public int? KeepAliveSeconds { get; set; }

    [Required]
    public bool? AllowAdmin { get; set; }

    [Required]
    public string ClientName { get; set; } = null!;

    [Required]
    public string LockKeyPrefix { get; set; } = null!;

    [Required]
    public string AttemptKeyPrefix { get; set; } = null!;

    [Required]
    public string ProcessedKeyPrefix { get; set; } = null!;

    [Required]
    public string LastEventKeyPrefix { get; set; } = null!;

    public string? Password { get; set; }
}
