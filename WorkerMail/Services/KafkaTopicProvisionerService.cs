using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using WorkerMail.Options;

namespace WorkerMail.Services;

public sealed class KafkaTopicProvisionerService
{
    private readonly IAdminClient _adminClient;
    private readonly ILogger<KafkaTopicProvisionerService> _logger;
    private readonly KafkaOptions _kafkaOptions;

    public KafkaTopicProvisionerService(
        IAdminClient adminClient,
        IOptions<KafkaOptions> kafkaOptions,
        ILogger<KafkaTopicProvisionerService> logger)
    {
        _adminClient = adminClient;
        _logger = logger;
        _kafkaOptions = kafkaOptions.Value;
    }

    public async Task EnsureTopicsAvailableAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (_kafkaOptions.EnsureTopicsOnStartup!.Value)
                {
                    await TryCreateTopicsAsync(cancellationToken);
                }

                IReadOnlyList<string> missingTopics = GetMissingTopics();
                if (missingTopics.Count == 0)
                {
                    return;
                }

                _logger.LogWarning(
                    "Os tópicos Kafka ainda não estão disponíveis: {Topics}. Nova tentativa em {DelayMs} ms.",
                    string.Join(", ", missingTopics),
                    _kafkaOptions.TopicProvisionRetryDelayMs!.Value);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (KafkaException ex)
            {
                await Core.Log.EnqueueWarningAsync(
                    "Kafka indisponível ao validar ou criar tópicos do WorkerMail.",
                    ex,
                    new Dictionary<string, string>
                    {
                        ["worker"] = "WorkerMail",
                        ["requestTopic"] = _kafkaOptions.RequestTopic,
                        ["deadLetterTopic"] = _kafkaOptions.DeadLetterTopic
                    });

                _logger.LogWarning(
                    ex,
                    "Kafka indisponível ao validar/criar tópicos. Nova tentativa em {DelayMs} ms.",
                    _kafkaOptions.TopicProvisionRetryDelayMs!.Value);
            }
            catch (Exception ex)
            {
                await Core.Log.EnqueueErrorAsync(
                    "Erro inesperado ao validar ou criar tópicos Kafka do WorkerMail.",
                    ex,
                    new Dictionary<string, string>
                    {
                        ["worker"] = "WorkerMail",
                        ["requestTopic"] = _kafkaOptions.RequestTopic,
                        ["deadLetterTopic"] = _kafkaOptions.DeadLetterTopic
                    });

                _logger.LogWarning(
                    ex,
                    "Erro inesperado ao validar/criar tópicos Kafka. Nova tentativa em {DelayMs} ms.",
                    _kafkaOptions.TopicProvisionRetryDelayMs!.Value);
            }

            await Task.Delay(_kafkaOptions.TopicProvisionRetryDelayMs.Value, cancellationToken);
        }
    }

    private async Task TryCreateTopicsAsync(CancellationToken cancellationToken)
    {
        List<TopicSpecification> topicSpecifications =
        [
            new TopicSpecification
            {
                Name = _kafkaOptions.RequestTopic,
                NumPartitions = _kafkaOptions.RequestTopicPartitions!.Value,
                ReplicationFactor = _kafkaOptions.RequestTopicReplicationFactor!.Value
            },
            new TopicSpecification
            {
                Name = _kafkaOptions.DeadLetterTopic,
                NumPartitions = _kafkaOptions.DeadLetterTopicPartitions!.Value,
                ReplicationFactor = _kafkaOptions.DeadLetterTopicReplicationFactor!.Value
            }
        ];

        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            await _adminClient.CreateTopicsAsync(topicSpecifications);
            _logger.LogInformation(
                "Tópicos Kafka garantidos com sucesso: {Topics}",
                string.Join(", ", topicSpecifications.Select(topic => topic.Name)));
        }
        catch (CreateTopicsException ex)
        {
            List<string> errors = [];

            foreach (CreateTopicReport report in ex.Results)
            {
                if (report.Error.Code == ErrorCode.TopicAlreadyExists)
                {
                    continue;
                }

                errors.Add($"{report.Topic}: {report.Error.Reason}");
            }

            if (errors.Count == 0)
            {
                return;
            }

            _logger.LogWarning(
                "Não foi possível criar um ou mais tópicos Kafka automaticamente: {Errors}",
                string.Join(" | ", errors));
        }
    }

    private IReadOnlyList<string> GetMissingTopics()
    {
        Metadata metadata = _adminClient.GetMetadata(TimeSpan.FromMilliseconds(_kafkaOptions.TopicMetadataTimeoutMs!.Value));
        HashSet<string> availableTopics = metadata.Topics
            .Where(topic => topic.Error.Code == ErrorCode.NoError)
            .Select(topic => topic.Topic)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        List<string> requiredTopics =
        [
            _kafkaOptions.RequestTopic,
            _kafkaOptions.DeadLetterTopic
        ];

        return requiredTopics
            .Where(topic => !availableTopics.Contains(topic))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }
}
