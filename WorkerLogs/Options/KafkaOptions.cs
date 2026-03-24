using System.ComponentModel.DataAnnotations;

namespace WorkerLogs.Options;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    [Required]
    public string BootstrapServers { get; set; } = null!;

    [Required]
    public string TopicName { get; set; } = null!;

    [Required]
    public string GroupId { get; set; } = null!;

    [Required]
    public string ConsumerClientId { get; set; } = null!;

    [Required]
    public string AdminClientId { get; set; } = null!;

    [Required]
    public string AutoOffsetReset { get; set; } = null!;

    [Required]
    public bool? EnableAutoCommit { get; set; }

    [Required]
    public bool? ConsumerAllowAutoCreateTopics { get; set; }

    [Required]
    public bool? EnsureTopicOnStartup { get; set; }

    [Required]
    [Range(1000, 120000)]
    public int? TopicProvisionRetryDelayMs { get; set; }

    [Required]
    [Range(1000, 120000)]
    public int? TopicMetadataTimeoutMs { get; set; }

    [Required]
    [Range(1000, 120000)]
    public int? SessionTimeoutMs { get; set; }

    [Required]
    [Range(1000, 120000)]
    public int? SocketTimeoutMs { get; set; }

    [Required]
    [Range(1, 1000)]
    public int? TopicPartitions { get; set; }

    [Required]
    [Range(1, 100)]
    public short? TopicReplicationFactor { get; set; }

    public string? SecurityProtocol { get; set; }
    public string? SaslMechanism { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
}
