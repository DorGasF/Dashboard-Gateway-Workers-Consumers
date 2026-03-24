using Confluent.Kafka;
using WorkerMail.Models;
using WorkerMail.Options;
using WorkerMail.Services;

namespace WorkerMail;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly KafkaTopicProvisionerService _kafkaTopicProvisionerService;
    private readonly MailProcessingService _mailProcessingService;
    private readonly string _requestTopic;
    private readonly string _groupId;
    private readonly int _pollIntervalMs;
    private readonly int _idleDelayMs;
    private readonly int _retryDelayMs;

    public Worker(
        ILogger<Worker> logger,
        IConsumer<string, string> consumer,
        KafkaTopicProvisionerService kafkaTopicProvisionerService,
        MailProcessingService mailProcessingService,
        Microsoft.Extensions.Options.IOptions<KafkaOptions> kafkaOptions,
        Microsoft.Extensions.Options.IOptions<WorkerOptions> workerOptions)
    {
        _logger = logger;
        _consumer = consumer;
        _kafkaTopicProvisionerService = kafkaTopicProvisionerService;
        _mailProcessingService = mailProcessingService;
        _requestTopic = kafkaOptions.Value.RequestTopic;
        _groupId = kafkaOptions.Value.GroupId;
        _pollIntervalMs = workerOptions.Value.PollIntervalMs!.Value;
        _idleDelayMs = workerOptions.Value.IdleDelayMs!.Value;
        _retryDelayMs = workerOptions.Value.RetryDelayMs!.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            _logger.LogInformation(
                "WorkerMail iniciado. Tópico principal: {Topic}. Grupo Kafka: {GroupId}",
                _requestTopic,
                _groupId);

            await _kafkaTopicProvisionerService.EnsureTopicsAvailableAsync(stoppingToken);
            _consumer.Subscribe(_requestTopic);

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? consumeResult = null;

                try
                {
                    consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(_pollIntervalMs));

                    if (consumeResult is null)
                    {
                        await Task.Delay(_idleDelayMs, stoppingToken);
                        continue;
                    }

                    MailProcessingResult processingResult = await _mailProcessingService.ProcessAsync(consumeResult, stoppingToken);

                    if (processingResult.Action == MailProcessingAction.Commit)
                    {
                        _consumer.Commit(consumeResult);
                        continue;
                    }

                    RetryCurrentMessage(consumeResult, processingResult.Reason);
                    await Task.Delay(_retryDelayMs, stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    if (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                    {
                        _logger.LogWarning(
                            ex,
                            "O tópico Kafka {Topic} ainda não está disponível. O worker aguardará a criação/propagação do tópico.",
                            _requestTopic);

                        await _kafkaTopicProvisionerService.EnsureTopicsAvailableAsync(stoppingToken);
                        continue;
                    }

                    _logger.LogError(ex, "Erro ao consumir mensagem do Kafka");
                    await Task.Delay(_retryDelayMs, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro inesperado no WorkerMail");

                    if (consumeResult is not null)
                    {
                        RetryCurrentMessage(consumeResult, "Falha inesperada durante o processamento.");
                    }

                    await Task.Delay(_retryDelayMs, stoppingToken);
                }
            }
        }
        finally
        {
            _consumer.Close();
        }
    }

    private void RetryCurrentMessage(ConsumeResult<string, string> consumeResult, string reason)
    {
        _logger.LogWarning(
            "Mensagem será reprocessada. Partition: {Partition}, Offset: {Offset}. Motivo: {Reason}",
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            reason);

        _consumer.Seek(consumeResult.TopicPartitionOffset);
    }
}
