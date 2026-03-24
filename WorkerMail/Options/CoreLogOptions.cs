using System.ComponentModel.DataAnnotations;

namespace WorkerMail.Options;

public sealed class CoreLogOptions
{
    public const string SectionName = "CoreLog";

    [Required]
    public bool? Enabled { get; set; }

    [Required]
    public string ApplicationName { get; set; } = null!;

    [Required]
    public string SourceType { get; set; } = null!;

    [Required]
    public string BootstrapServers { get; set; } = null!;

    [Required]
    public string TopicName { get; set; } = null!;

    [Required]
    public string ProducerClientId { get; set; } = null!;

    [Required]
    public string AdminClientId { get; set; } = null!;

    [Required]
    public bool? EnsureTopicOnStartup { get; set; }

    [Required]
    [Range(1, 1000)]
    public int? TopicPartitions { get; set; }

    [Required]
    [Range(1, 100)]
    public short? TopicReplicationFactor { get; set; }

    [Required]
    [Range(1000, 120000)]
    public int? TopicMetadataTimeoutMs { get; set; }

    [Required]
    [Range(1000, 120000)]
    public int? SocketTimeoutMs { get; set; }

    public string? SecurityProtocol { get; set; }
    public string? SaslMechanism { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
}
