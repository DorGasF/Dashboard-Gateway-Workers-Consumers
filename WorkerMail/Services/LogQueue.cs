using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using System.Text.Json;
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
            SocketTimeoutMs = _options.SocketTimeoutMs!.Value
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
    }

    public async Task EnsureTopicAsync(CancellationToken cancellationToken = default)
    {
        if (!_options.Enabled!.Value || !_options.EnsureTopicOnStartup!.Value || _adminClient is null)
        {
            return;
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();

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

    public async Task EnqueueAsync(
        LogLevel level,
        string message,
        Exception? ex = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        [CallerMemberName] string memberName = "",
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0)
    {
        if (!_options.Enabled!.Value || _producer is null)
        {
            return;
        }

        try
        {
            CoreLogEntry entry = new()
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

            string payload = JsonSerializer.Serialize(entry, JsonOptions);

            await _producer.ProduceAsync(
                _options.TopicName,
                new Message<string, string>
                {
                    Key = entry.LogId.ToString("N"),
                    Value = payload
                });
        }
        catch (Exception innerEx)
        {
            _logger.LogError(innerEx, "Falha ao enfileirar log central no Kafka.");
        }
    }

    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();
        _adminClient?.Dispose();
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
}
