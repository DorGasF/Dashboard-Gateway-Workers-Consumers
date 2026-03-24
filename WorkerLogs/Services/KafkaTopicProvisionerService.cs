using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using WorkerLogs.Options;

namespace WorkerLogs.Services;

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

    public async Task EnsureTopicAvailableAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (_kafkaOptions.EnsureTopicOnStartup!.Value)
                {
                    await TryCreateTopicAsync(cancellationToken);
                }

                if (TopicExists())
                {
                    return;
                }

                _logger.LogWarning(
                    "O tópico Kafka de logs {Topic} ainda não está disponível. Nova tentativa em {DelayMs} ms.",
                    _kafkaOptions.TopicName,
                    _kafkaOptions.TopicProvisionRetryDelayMs!.Value);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (KafkaException ex)
            {
                _logger.LogWarning(
                    ex,
                    "Kafka indisponível ao validar ou criar o tópico de logs. Nova tentativa em {DelayMs} ms.",
                    _kafkaOptions.TopicProvisionRetryDelayMs!.Value);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Erro inesperado ao validar ou criar o tópico de logs. Nova tentativa em {DelayMs} ms.",
                    _kafkaOptions.TopicProvisionRetryDelayMs!.Value);
            }

            await Task.Delay(_kafkaOptions.TopicProvisionRetryDelayMs.Value, cancellationToken);
        }
    }

    private async Task TryCreateTopicAsync(CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _adminClient.CreateTopicsAsync(
            [
                new TopicSpecification
                {
                    Name = _kafkaOptions.TopicName,
                    NumPartitions = _kafkaOptions.TopicPartitions!.Value,
                    ReplicationFactor = _kafkaOptions.TopicReplicationFactor!.Value
                }
            ]);
        }
        catch (CreateTopicsException ex)
        {
            bool onlyAlreadyExists = ex.Results.All(result => result.Error.Code == ErrorCode.TopicAlreadyExists);
            if (!onlyAlreadyExists)
            {
                _logger.LogWarning(ex, "Não foi possível criar automaticamente o tópico de logs {Topic}.", _kafkaOptions.TopicName);
            }
        }
    }

    private bool TopicExists()
    {
        Metadata metadata = _adminClient.GetMetadata(TimeSpan.FromMilliseconds(_kafkaOptions.TopicMetadataTimeoutMs!.Value));
        return metadata.Topics.Any(topic =>
            string.Equals(topic.Topic, _kafkaOptions.TopicName, StringComparison.OrdinalIgnoreCase) &&
            topic.Error.Code == ErrorCode.NoError);
    }
}
