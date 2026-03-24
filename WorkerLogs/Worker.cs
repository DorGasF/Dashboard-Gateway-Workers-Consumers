using Confluent.Kafka;
using System.Text;
using System.Text.Json;
using WorkerLogs.Models;
using WorkerLogs.Options;
using WorkerLogs.Services;

namespace WorkerLogs;

public sealed class Worker : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private readonly ILogger<Worker> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly KafkaTopicProvisionerService _topicProvisionerService;
    private readonly string _topicName;
    private readonly int _batchSize;
    private readonly int _flushIntervalMs;
    private readonly int _retryDelayMs;
    private readonly string _logsDirectory;
    private readonly List<string> _buffer = [];
    private readonly object _lock = new();
    private static int _isFlushing;

    public Worker(
        ILogger<Worker> logger,
        IConsumer<string, string> consumer,
        KafkaTopicProvisionerService topicProvisionerService,
        Microsoft.Extensions.Options.IOptions<KafkaOptions> kafkaOptions,
        Microsoft.Extensions.Options.IOptions<WorkerOptions> workerOptions,
        Microsoft.Extensions.Options.IOptions<StorageOptions> storageOptions)
    {
        _logger = logger;
        _consumer = consumer;
        _topicProvisionerService = topicProvisionerService;
        _topicName = kafkaOptions.Value.TopicName;
        _batchSize = workerOptions.Value.BatchSize!.Value;
        _flushIntervalMs = workerOptions.Value.FlushIntervalMs!.Value;
        _retryDelayMs = workerOptions.Value.RetryDelayMs!.Value;
        _logsDirectory = Path.Combine(AppContext.BaseDirectory, storageOptions.Value.LogsDirectory);
        Directory.CreateDirectory(_logsDirectory);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "WorkerLogs iniciado. Tópico Kafka: {Topic}. Batch: {BatchSize}. Flush: {FlushInterval} ms",
            _topicName,
            _batchSize,
            _flushIntervalMs);

        await _topicProvisionerService.EnsureTopicAvailableAsync(stoppingToken);
        _consumer.Subscribe(_topicName);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<string, string>? consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(_flushIntervalMs));

                    if (consumeResult is null)
                    {
                        _ = Task.Run(FlushToDiskAsync, stoppingToken);
                        await Task.Delay(_flushIntervalMs, stoppingToken);
                        continue;
                    }

                    ProcessLogEntry(consumeResult.Message.Value);
                    _consumer.Commit(consumeResult);

                    if (_buffer.Count >= _batchSize)
                    {
                        _ = Task.Run(FlushToDiskAsync, stoppingToken);
                    }
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                {
                    _logger.LogWarning(
                        ex,
                        "Tópico de logs {Topic} ainda não disponível. O WorkerLogs aguardará e tentará novamente.",
                        _topicName);

                    await _topicProvisionerService.EnsureTopicAvailableAsync(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Erro ao consumir logs do Kafka");
                    await Task.Delay(_retryDelayMs, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro inesperado ao processar logs");
                    await Task.Delay(_retryDelayMs, stoppingToken);
                }
            }
        }
        finally
        {
            await FlushToDiskAsync();
            _consumer.Close();
            _consumer.Dispose();
        }
    }

    private void ProcessLogEntry(string? rawMessage)
    {
        if (string.IsNullOrWhiteSpace(rawMessage))
        {
            return;
        }

        try
        {
            CoreLogEntry? logEntry = JsonSerializer.Deserialize<CoreLogEntry>(rawMessage, JsonOptions);
            if (logEntry is null)
            {
                return;
            }

            string line = FormatLogLine(logEntry);

            lock (_lock)
            {
                _buffer.Add(line);
            }
        }
        catch
        {
            string fallbackLine = $"[{DateTimeOffset.UtcNow:O}] [Warning] [WorkerLogs] Payload de log inválido: {rawMessage}";
            lock (_lock)
            {
                _buffer.Add(fallbackLine);
            }
        }
    }

    private async Task FlushToDiskAsync()
    {
        if (Interlocked.Exchange(ref _isFlushing, 1) == 1)
        {
            return;
        }

        List<string> lines;
        lock (_lock)
        {
            if (_buffer.Count == 0)
            {
                Interlocked.Exchange(ref _isFlushing, 0);
                return;
            }

            lines = [.. _buffer];
            _buffer.Clear();
        }

        try
        {
            string filePath = Path.Combine(_logsDirectory, $"{DateTime.UtcNow:yyyy-MM-dd}.log");
            await File.AppendAllLinesAsync(filePath, lines, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Erro ao gravar logs em disco");
        }
        finally
        {
            Interlocked.Exchange(ref _isFlushing, 0);
        }
    }

    private static string FormatLogLine(CoreLogEntry entry)
    {
        string fileName = string.IsNullOrWhiteSpace(entry.FilePath) ? string.Empty : Path.GetFileName(entry.FilePath);
        string source = string.IsNullOrWhiteSpace(entry.ApplicationName) ? "Desconhecido" : entry.ApplicationName;
        string location = string.IsNullOrWhiteSpace(entry.MemberName)
            ? string.Empty
            : $" [{entry.MemberName} {fileName}:{entry.LineNumber}]";

        string metadata = entry.Metadata.Count == 0
            ? string.Empty
            : $" | META: {string.Join(", ", entry.Metadata.OrderBy(item => item.Key).Select(item => $"{item.Key}={item.Value}"))}";

        string exception = string.IsNullOrWhiteSpace(entry.Exception)
            ? string.Empty
            : $" | EX: {entry.Exception}";

        return $"[{entry.Timestamp:O}] [{entry.Level}] [{source}/{entry.SourceType}]{location} {entry.Message}{metadata}{exception}";
    }
}
