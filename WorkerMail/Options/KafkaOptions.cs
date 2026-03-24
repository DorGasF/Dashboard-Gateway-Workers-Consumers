using System.ComponentModel.DataAnnotations;

namespace WorkerMail.Options;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    [Required]
    public string BootstrapServers { get; set; } = null!;

    [Required]
    public string GroupId { get; set; } = null!;

    [Required]
    public string RequestTopic { get; set; } = null!;

    [Required]
    public string DeadLetterTopic { get; set; } = null!;

    [Required]
    public string ConsumerClientId { get; set; } = null!;

    [Required]
    public string ProducerClientId { get; set; } = null!;

    [Required]
    public string AutoOffsetReset { get; set; } = null!;

    [Required]
    public bool? EnableAutoCommit { get; set; }

    [Required]
    public bool? ConsumerAllowAutoCreateTopics { get; set; }

    [Required]
    public bool? ProducerAllowAutoCreateTopics { get; set; }

    [Required]
    [Range(1000, 120000)]
    public int? SessionTimeoutMs { get; set; }

    [Required]
    [Range(1000, 120000)]
    public int? SocketTimeoutMs { get; set; }

    public string? SecurityProtocol { get; set; }
    public string? SaslMechanism { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
}
