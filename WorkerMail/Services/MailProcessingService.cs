using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Net;
using System.Net.Mail;
using System.Text.Json;
using System.Text.RegularExpressions;
using WorkerMail.Models;
using WorkerMail.Options;

namespace WorkerMail.Services;

public sealed class MailProcessingService
{
    private static readonly Regex TemplateNameRegex = new("^[a-zA-Z0-9._-]+$", RegexOptions.Compiled);
    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };

    private readonly ILogger<MailProcessingService> _logger;
    private readonly RedisService _redisService;
    private readonly TemplateRendererService _templateRendererService;
    private readonly SmtpEmailSender _smtpEmailSender;
    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _kafkaOptions;
    private readonly string _instanceId = $"{Environment.MachineName}-{Environment.ProcessId}";
    private readonly int _lockTimeoutSeconds;
    private readonly int _attemptKeyTtlHours;
    private readonly int _processedKeyTtlHours;
    private readonly int _maxProcessingAttempts;
    private readonly bool _cacheLastEvent;
    private readonly int _lastEventCacheTtlMinutes;

    public MailProcessingService(
        ILogger<MailProcessingService> logger,
        RedisService redisService,
        TemplateRendererService templateRendererService,
        SmtpEmailSender smtpEmailSender,
        IProducer<string, string> producer,
        IOptions<KafkaOptions> kafkaOptions,
        IOptions<WorkerOptions> workerOptions)
    {
        _logger = logger;
        _redisService = redisService;
        _templateRendererService = templateRendererService;
        _smtpEmailSender = smtpEmailSender;
        _producer = producer;
        _kafkaOptions = kafkaOptions.Value;
        _lockTimeoutSeconds = workerOptions.Value.LockTimeoutSeconds!.Value;
        _attemptKeyTtlHours = workerOptions.Value.AttemptKeyTtlHours!.Value;
        _processedKeyTtlHours = workerOptions.Value.ProcessedKeyTtlHours!.Value;
        _maxProcessingAttempts = workerOptions.Value.MaxProcessingAttempts!.Value;
        _cacheLastEvent = workerOptions.Value.CacheLastEvent!.Value;
        _lastEventCacheTtlMinutes = workerOptions.Value.LastEventCacheTtlMinutes!.Value;
    }

    public async Task<MailProcessingResult> ProcessAsync(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
    {
        string rawMessage = consumeResult.Message.Value ?? string.Empty;
        MailEvent? mailEvent;

        try
        {
            mailEvent = JsonSerializer.Deserialize<MailEvent>(rawMessage, JsonSerializerOptions);
        }
        catch (JsonException ex)
        {
            await PublishDeadLetterAsync(
                rawMessage,
                mailEvent: null,
                consumeResult,
                "payload_invalido",
                ex.Message,
                attemptCount: 0,
                cancellationToken);

            return MailProcessingResult.Commit("Payload inválido enviado para a DLQ.");
        }

        if (!TryValidateEvent(mailEvent, out string validationError))
        {
            await PublishDeadLetterAsync(
                rawMessage,
                mailEvent,
                consumeResult,
                "evento_invalido",
                validationError,
                attemptCount: 0,
                cancellationToken);

            return MailProcessingResult.Commit("Evento inválido enviado para a DLQ.");
        }

        string idempotencyKey = mailEvent!.ResolveIdempotencyKey();
        if (await _redisService.IsProcessedAsync(idempotencyKey))
        {
            _logger.LogInformation("Evento {EventId} já foi processado anteriormente. Commitando sem reenviar.", mailEvent.EventId);
            return MailProcessingResult.Commit("Evento já processado.");
        }

        string lockOwner = $"{_instanceId}:{consumeResult.Partition.Value}:{consumeResult.Offset.Value}";
        bool lockAcquired = await _redisService.TryAcquireProcessingLockAsync(
            idempotencyKey,
            lockOwner,
            TimeSpan.FromSeconds(_lockTimeoutSeconds));

        if (!lockAcquired)
        {
            return MailProcessingResult.Retry("Outro worker está processando esse mesmo evento.");
        }

        int attempt = 0;

        try
        {
            if (await _redisService.IsProcessedAsync(idempotencyKey))
            {
                return MailProcessingResult.Commit("Evento já processado após aquisição do lock.");
            }

            attempt = await _redisService.IncrementAttemptAsync(
                idempotencyKey,
                TimeSpan.FromHours(_attemptKeyTtlHours));

            RenderedMail renderedMail = await _templateRendererService.RenderAsync(mailEvent, cancellationToken);
            string messageId = await _smtpEmailSender.SendAsync(mailEvent, renderedMail, cancellationToken);

            await _redisService.MarkAsProcessedAsync(
                idempotencyKey,
                new
                {
                    mailEvent.EventId,
                    mailEvent.EventType,
                    mailEvent.To,
                    MessageId = messageId,
                    ProcessedAt = DateTimeOffset.UtcNow,
                    Attempt = attempt
                },
                TimeSpan.FromHours(_processedKeyTtlHours));

            await _redisService.ClearAttemptAsync(idempotencyKey);

            if (_cacheLastEvent)
            {
                await _redisService.CacheLastEventAsync(mailEvent, TimeSpan.FromMinutes(_lastEventCacheTtlMinutes));
            }

            _logger.LogInformation(
                "Evento {EventId} processado com sucesso para {To}. Tentativa {Attempt}",
                mailEvent.EventId,
                mailEvent.To,
                attempt);

            return MailProcessingResult.Commit("E-mail enviado com sucesso.");
        }
        catch (InvalidOperationException ex)
        {
            await PublishDeadLetterAsync(
                rawMessage,
                mailEvent,
                consumeResult,
                "template_invalido",
                ex.Message,
                attempt,
                cancellationToken);

            await _redisService.ClearAttemptAsync(idempotencyKey);

            return MailProcessingResult.Commit("Falha permanente de template enviada para a DLQ.");
        }
        catch (SmtpException ex) when (attempt >= _maxProcessingAttempts)
        {
            await PublishDeadLetterAsync(
                rawMessage,
                mailEvent,
                consumeResult,
                "smtp_falhou_limite",
                ex.Message,
                attempt,
                cancellationToken);

            await _redisService.ClearAttemptAsync(idempotencyKey);

            return MailProcessingResult.Commit("Falha SMTP enviada para a DLQ após atingir o limite.");
        }
        catch (SmtpException ex)
        {
            _logger.LogWarning(
                ex,
                "Falha SMTP temporária ao enviar evento {EventId}. Tentativa {Attempt} de {MaxAttempts}",
                mailEvent.EventId,
                attempt,
                _maxProcessingAttempts);

            return MailProcessingResult.Retry("Falha SMTP temporária.");
        }
        catch (Exception ex) when (attempt >= _maxProcessingAttempts)
        {
            await PublishDeadLetterAsync(
                rawMessage,
                mailEvent,
                consumeResult,
                "processamento_falhou_limite",
                ex.Message,
                attempt,
                cancellationToken);

            await _redisService.ClearAttemptAsync(idempotencyKey);

            return MailProcessingResult.Commit("Falha de processamento enviada para a DLQ após atingir o limite.");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Falha temporária ao processar evento {EventId}. Tentativa {Attempt} de {MaxAttempts}",
                mailEvent.EventId,
                attempt,
                _maxProcessingAttempts);

            return MailProcessingResult.Retry("Falha temporária durante o processamento.");
        }
        finally
        {
            await _redisService.ReleaseProcessingLockAsync(idempotencyKey, lockOwner);
        }
    }

    private async Task PublishDeadLetterAsync(
        string rawMessage,
        MailEvent? mailEvent,
        ConsumeResult<string, string> consumeResult,
        string reason,
        string errorMessage,
        int attemptCount,
        CancellationToken cancellationToken)
    {
        DeadLetterMailEvent deadLetterEvent = new()
        {
            Reason = reason,
            ErrorMessage = errorMessage,
            AttemptCount = attemptCount,
            SourceTopic = consumeResult.Topic,
            SourcePartition = consumeResult.Partition.Value,
            SourceOffset = consumeResult.Offset.Value,
            RawMessage = rawMessage,
            MailEvent = mailEvent
        };

        string serializedDeadLetter = JsonSerializer.Serialize(deadLetterEvent, JsonSerializerOptions);

        await _producer.ProduceAsync(
            _kafkaOptions.DeadLetterTopic,
            new Message<string, string>
            {
                Key = mailEvent?.ResolveIdempotencyKey() ?? consumeResult.Message.Key ?? Guid.NewGuid().ToString("N"),
                Value = serializedDeadLetter
            },
            cancellationToken);

        _logger.LogError(
            "Evento enviado para a DLQ. Motivo: {Reason}. Tópico origem: {SourceTopic}. Offset: {Offset}",
            reason,
            consumeResult.Topic,
            consumeResult.Offset.Value);
    }

    private static bool TryValidateEvent(MailEvent? mailEvent, out string validationError)
    {
        if (mailEvent is null)
        {
            validationError = "Evento nulo.";
            return false;
        }

        if (mailEvent.EventId == Guid.Empty)
        {
            validationError = "EventId é obrigatório.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(mailEvent.ResolveIdempotencyKey()))
        {
            validationError = "IdempotencyKey é obrigatório.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(mailEvent.Template))
        {
            validationError = "Template é obrigatório.";
            return false;
        }

        if (!TemplateNameRegex.IsMatch(mailEvent.Template))
        {
            validationError = "Template possui caracteres inválidos.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(mailEvent.To))
        {
            validationError = "Destinatário é obrigatório.";
            return false;
        }

        if (!TryValidateAddresses(mailEvent.To, mailEvent.Cc, mailEvent.Bcc, out validationError))
        {
            return false;
        }

        validationError = string.Empty;
        return true;
    }

    private static bool TryValidateAddresses(string to, IEnumerable<string> cc, IEnumerable<string> bcc, out string validationError)
    {
        try
        {
            List<string> toAddresses = SplitAddresses(to).ToList();
            if (toAddresses.Count == 0)
            {
                validationError = "Destinatário é obrigatório.";
                return false;
            }

            foreach (string address in toAddresses.Concat(cc).Concat(bcc))
            {
                _ = new MailAddress(address);
            }

            validationError = string.Empty;
            return true;
        }
        catch (FormatException)
        {
            validationError = "Existe pelo menos um endereço de e-mail inválido no evento.";
            return false;
        }
    }

    private static IEnumerable<string> SplitAddresses(string addresses)
    {
        return addresses
            .Split([';', ','], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }
}
