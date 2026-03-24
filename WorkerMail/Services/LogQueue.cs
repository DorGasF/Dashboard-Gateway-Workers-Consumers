using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using WorkerMail.Models;
using WorkerMail.Options;

namespace WorkerMail.Services;

public sealed class LogQueue : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    private readonly ILogger<LogQueue> _logger;
    private readonly CoreLogOptions _options;
    private readonly IProducer<string, string>? _producer;
    private readonly IAdminClient? _adminClient;
    private readonly Channel<CoreLogEntry>? _channel;
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _flushSignal = new(0, 1);
    private readonly Task? _backgroundTask;
    private DateTimeOffset _nextTopicEnsureAttemptUtc = DateTimeOffset.MinValue;
    private volatile bool _topicReady;
    private int _queuedCount;
    private int _signalPending;

    public LogQueue(IOptions<CoreLogOptions> options, ILogger<LogQueue> logger)
    {
        _logger = logger;
        _options = options.Value;

        if (!_options.Enabled!.Value)
        {
            return;
        }

        ProducerConfig producerConfig = new()
        {
            BootstrapServers = _options.BootstrapServers,
            AllowAutoCreateTopics = false,
            ClientId = _options.ProducerClientId,
            SocketTimeoutMs = _options.SocketTimeoutMs!.Value,
            LingerMs = _options.LingerMs!.Value,
            BatchSize = _options.BatchSizeBytes!.Value,
            EnableIdempotence = _options.EnableIdempotence!.Value,
            MessageSendMaxRetries = _options.MessageSendMaxRetries!.Value,
            RetryBackoffMs = _options.RetryBackoffMs!.Value,
            Acks = ParseAcks(_options.Acks),
            CompressionType = ParseCompressionType(_options.CompressionType)
        };

        AdminClientConfig adminClientConfig = new()
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = _options.AdminClientId,
            SocketTimeoutMs = _options.SocketTimeoutMs!.Value
        };

        ApplyKafkaSecurity(producerConfig);
        ApplyKafkaSecurity(adminClientConfig);

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        _adminClient = new AdminClientBuilder(adminClientConfig).Build();
        _channel = Channel.CreateBounded<CoreLogEntry>(new BoundedChannelOptions(_options.QueueCapacity!.Value)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false
        });

        _backgroundTask = Task.Run(ProcessQueueAsync);
    }

    public Task EnqueueInfoAsync(
        string message,
        IReadOnlyDictionary<string, string>? metadata = null,
        [CallerMemberName] string memberName = "",
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0)
        => EnqueueAsync(LogLevel.Information, message, null, metadata, memberName, filePath, lineNumber);

    public Task EnqueueWarningAsync(
        string message,
        Exception? ex = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        [CallerMemberName] string memberName = "",
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0)
        => EnqueueAsync(LogLevel.Warning, message, ex, metadata, memberName, filePath, lineNumber);

    public Task EnqueueErrorAsync(
        string message,
        Exception? ex = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        [CallerMemberName] string memberName = "",
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0)
        => EnqueueAsync(LogLevel.Error, message, ex, metadata, memberName, filePath, lineNumber);

    public Task EnqueueAsync(
        LogLevel level,
        string message,
        Exception? ex = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        [CallerMemberName] string memberName = "",
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0)
    {
        if (!_options.Enabled!.Value)
        {
            return Task.CompletedTask;
        }

        CoreLogEntry entry = BuildEntry(level, message, ex, metadata, memberName, filePath, lineNumber);

        if (_channel is null)
        {
            WriteFallbackLog(entry, "fila_indisponivel");
            return Task.CompletedTask;
        }

        if (!_channel.Writer.TryWrite(entry))
        {
            WriteFallbackLog(entry, "fila_cheia");
            return Task.CompletedTask;
        }

        Interlocked.Increment(ref _queuedCount);

        if (Volatile.Read(ref _queuedCount) >= _options.MaxBatchSize!.Value &&
            Interlocked.Exchange(ref _signalPending, 1) == 0)
        {
            _flushSignal.Release();
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        try
        {
            _channel?.Writer.TryComplete();

            if (Interlocked.Exchange(ref _signalPending, 1) == 0)
            {
                _flushSignal.Release();
            }

            if (_backgroundTask is not null && !_backgroundTask.Wait(TimeSpan.FromSeconds(5)))
            {
                _cts.Cancel();
                _backgroundTask.Wait(TimeSpan.FromSeconds(5));
            }
        }
        catch
        {
        }
        finally
        {
            _producer?.Flush(TimeSpan.FromSeconds(5));
            _producer?.Dispose();
            _adminClient?.Dispose();
            _cts.Cancel();
            _cts.Dispose();
            _flushSignal.Dispose();
        }
    }

    private async Task ProcessQueueAsync()
    {
        if (_channel is null || _producer is null)
        {
            return;
        }

        try
        {
            while (!_cts.IsCancellationRequested)
            {
                if (_channel.Reader.Completion.IsCompleted && Volatile.Read(ref _queuedCount) == 0)
                {
                    break;
                }

                try
                {
                    Task delayTask = Task.Delay(_options.FlushIntervalMs!.Value, _cts.Token);
                    Task signalTask = _flushSignal.WaitAsync(_cts.Token);
                    await Task.WhenAny(delayTask, signalTask);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                finally
                {
                    Interlocked.Exchange(ref _signalPending, 0);
                }

                while (!_cts.IsCancellationRequested)
                {
                    List<CoreLogEntry> batch = DrainBatch();
                    if (batch.Count == 0)
                    {
                        break;
                    }

                    bool topicAvailable = await EnsureTopicReadyOnceAsync(_cts.Token);
                    if (!topicAvailable)
                    {
                        foreach (CoreLogEntry entry in batch)
                        {
                            WriteFallbackLog(entry, "topico_indisponivel");
                        }

                        continue;
                    }

                    PublishBatch(batch);

                    if (batch.Count < _options.MaxBatchSize!.Value)
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            List<CoreLogEntry> remaining = DrainBatch();
            foreach (CoreLogEntry entry in remaining)
            {
                WriteFallbackLog(entry, "flush_final");
            }
        }
    }

    private async Task<bool> EnsureTopicReadyOnceAsync(CancellationToken cancellationToken)
    {
        if (_topicReady)
        {
            return true;
        }

        if (_adminClient is null)
        {
            return false;
        }

        if (DateTimeOffset.UtcNow < _nextTopicEnsureAttemptUtc)
        {
            return false;
        }

        _nextTopicEnsureAttemptUtc = DateTimeOffset.UtcNow.AddMilliseconds(_options.TopicEnsureRetryDelayMs!.Value);

        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_options.EnsureTopicOnStartup!.Value)
            {
                await _adminClient.CreateTopicsAsync(
                [
                    new TopicSpecification
                    {
                        Name = _options.TopicName,
                        NumPartitions = _options.TopicPartitions!.Value,
                        ReplicationFactor = _options.TopicReplicationFactor!.Value
                    }
                ]);
            }
        }
        catch (CreateTopicsException ex)
        {
            bool onlyAlreadyExists = ex.Results.All(result => result.Error.Code == ErrorCode.TopicAlreadyExists);
            if (!onlyAlreadyExists)
            {
                _logger.LogWarning(ex, "Não foi possível garantir a criação do tópico de logs {Topic}.", _options.TopicName);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Falha ao garantir o tópico de logs {Topic}.", _options.TopicName);
        }

        try
        {
            Metadata metadata = _adminClient.GetMetadata(TimeSpan.FromMilliseconds(_options.TopicMetadataTimeoutMs!.Value));
            _topicReady = metadata.Topics.Any(topic =>
                string.Equals(topic.Topic, _options.TopicName, StringComparison.OrdinalIgnoreCase) &&
                topic.Error.Code == ErrorCode.NoError);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Falha ao consultar metadata do tópico de logs {Topic}.", _options.TopicName);
            _topicReady = false;
        }

        return _topicReady;
    }

    private List<CoreLogEntry> DrainBatch()
    {
        List<CoreLogEntry> batch = [];

        if (_channel is null)
        {
            return batch;
        }

        while (batch.Count < _options.MaxBatchSize!.Value && _channel.Reader.TryRead(out CoreLogEntry? entry))
        {
            batch.Add(entry);
            Interlocked.Decrement(ref _queuedCount);
        }

        return batch;
    }

    private void PublishBatch(List<CoreLogEntry> batch)
    {
        if (_producer is null)
        {
            foreach (CoreLogEntry entry in batch)
            {
                WriteFallbackLog(entry, "producer_indisponivel");
            }

            return;
        }

        foreach (CoreLogEntry entry in batch)
        {
            try
            {
                string payload = JsonSerializer.Serialize(entry, JsonOptions);

                _producer.Produce(
                    _options.TopicName,
                    new Message<string, string>
                    {
                        Key = entry.LogId.ToString("N"),
                        Value = payload
                    },
                    report =>
                    {
                        if (report.Error.IsError)
                        {
                            _topicReady = false;
                            WriteFallbackLog(entry, "falha_delivery_kafka");
                        }
                    });
            }
            catch (Exception ex)
            {
                _topicReady = false;
                WriteFallbackLog(entry, "falha_publicacao_kafka", ex);
            }
        }
    }

    private CoreLogEntry BuildEntry(
        LogLevel level,
        string message,
        Exception? ex,
        IReadOnlyDictionary<string, string>? metadata,
        string memberName,
        string filePath,
        int lineNumber)
    {
        return new CoreLogEntry
        {
            LogId = Guid.NewGuid(),
            Level = level.ToString(),
            Message = message,
            Exception = ex?.ToString(),
            ExceptionType = ex?.GetType().FullName,
            Timestamp = DateTimeOffset.UtcNow,
            ApplicationName = _options.ApplicationName,
            SourceType = _options.SourceType,
            MachineName = Environment.MachineName,
            ProcessId = Environment.ProcessId,
            MemberName = memberName,
            FilePath = filePath,
            LineNumber = lineNumber,
            Metadata = metadata is null
                ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, string>(metadata, StringComparer.OrdinalIgnoreCase)
        };
    }

    private void ApplyKafkaSecurity(ClientConfig clientConfig)
    {
        if (!string.IsNullOrWhiteSpace(_options.SecurityProtocol) &&
            Enum.TryParse(_options.SecurityProtocol, ignoreCase: true, out SecurityProtocol securityProtocol))
        {
            clientConfig.SecurityProtocol = securityProtocol;
        }

        if (!string.IsNullOrWhiteSpace(_options.SaslMechanism) &&
            Enum.TryParse(_options.SaslMechanism, ignoreCase: true, out SaslMechanism saslMechanism))
        {
            clientConfig.SaslMechanism = saslMechanism;
        }

        if (!string.IsNullOrWhiteSpace(_options.SaslUsername))
        {
            clientConfig.SaslUsername = _options.SaslUsername;
        }

        if (!string.IsNullOrWhiteSpace(_options.SaslPassword))
        {
            clientConfig.SaslPassword = _options.SaslPassword;
        }
    }

    private void WriteFallbackLog(CoreLogEntry entry, string reason, Exception? publishException = null)
    {
        string location = string.IsNullOrWhiteSpace(entry.MemberName)
            ? string.Empty
            : $"{Path.GetFileName(entry.FilePath)}:{entry.LineNumber} ({entry.MemberName})";

        string metadataText = entry.Metadata.Count == 0
            ? string.Empty
            : string.Join(", ", entry.Metadata.Select(item => $"{item.Key}={item.Value}"));

        Exception? exceptionToLog = publishException ?? (!string.IsNullOrWhiteSpace(entry.Exception)
            ? new Exception(entry.Exception)
            : null);

        _logger.Log(
            Enum.TryParse<LogLevel>(entry.Level, out LogLevel parsedLevel) ? parsedLevel : LogLevel.Error,
            exceptionToLog,
            "Core.Log fallback [{Reason}] {Message} | Local={Location} | Meta={Metadata}",
            reason,
            entry.Message,
            location,
            metadataText);
    }

    private static CompressionType ParseCompressionType(string value)
    {
        if (Enum.TryParse(value, ignoreCase: true, out CompressionType compressionType))
        {
            return compressionType;
        }

        return CompressionType.None;
    }

    private static Acks ParseAcks(string value)
    {
        if (Enum.TryParse(value, ignoreCase: true, out Acks acks))
        {
            return acks;
        }

        return Acks.Leader;
    }
}
