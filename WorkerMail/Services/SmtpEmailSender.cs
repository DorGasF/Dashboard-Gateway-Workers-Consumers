using Microsoft.Extensions.Options;
using System.Net;
using System.Net.Mail;
using System.Text;
using WorkerMail.Models;
using WorkerMail.Options;

namespace WorkerMail.Services;

public sealed class SmtpEmailSender
{
    private readonly SmtpOptions _smtpOptions;
    private readonly ILogger<SmtpEmailSender> _logger;

    public SmtpEmailSender(IOptions<SmtpOptions> smtpOptions, ILogger<SmtpEmailSender> logger)
    {
        _smtpOptions = smtpOptions.Value;
        _logger = logger;
    }

    public async Task<string> SendAsync(
        MailEvent mailEvent,
        RenderedMail renderedMail,
        SmtpSenderProfileOptions senderProfile,
        CancellationToken cancellationToken)
    {
        string messageId = BuildMessageId(mailEvent.ResolveIdempotencyKey(), senderProfile);

        using MailMessage message = new()
        {
            From = new MailAddress(senderProfile.FromEmail, senderProfile.FromName, Encoding.UTF8),
            Subject = renderedMail.Subject,
            SubjectEncoding = Encoding.UTF8,
            Body = renderedMail.Body,
            BodyEncoding = Encoding.UTF8,
            IsBodyHtml = renderedMail.IsHtml
        };

        AddAddresses(message.To, SplitAddresses(mailEvent.To));
        AddAddresses(message.CC, mailEvent.Cc);
        AddAddresses(message.Bcc, mailEvent.Bcc);

        if (!string.IsNullOrWhiteSpace(senderProfile.ReplyToEmail))
        {
            message.ReplyToList.Add(new MailAddress(senderProfile.ReplyToEmail, senderProfile.ReplyToName, Encoding.UTF8));
        }

        message.Headers.Add("X-Raims-Event-Id", mailEvent.EventId.ToString());
        message.Headers.Add("X-Raims-Idempotency-Key", mailEvent.ResolveIdempotencyKey());
        message.Headers.Add("Message-ID", messageId);

        if (_smtpOptions.DevelopmentMode!.Value)
        {
            _logger.LogInformation(
                "Envio SMTP simulado com sucesso para {To}. MailType: {MailType}. MessageId: {MessageId}",
                mailEvent.To,
                mailEvent.MailType ?? "legacy.template",
                messageId);

            return messageId;
        }

        using SmtpClient smtpClient = CreateClient();
        await smtpClient.SendMailAsync(message, cancellationToken);

        _logger.LogInformation("E-mail enviado com sucesso para {To}", mailEvent.To);
        return messageId;
    }

    private SmtpClient CreateClient()
    {
        SmtpClient smtpClient = new(_smtpOptions.Host, _smtpOptions.Port!.Value)
        {
            EnableSsl = _smtpOptions.EnableSsl!.Value,
            UseDefaultCredentials = _smtpOptions.UseDefaultCredentials!.Value,
            DeliveryMethod = SmtpDeliveryMethod.Network,
            Timeout = _smtpOptions.TimeoutMs!.Value
        };

        if (!_smtpOptions.UseDefaultCredentials.Value && !string.IsNullOrWhiteSpace(_smtpOptions.Username))
        {
            smtpClient.Credentials = new NetworkCredential(_smtpOptions.Username, _smtpOptions.Password);
        }

        return smtpClient;
    }

    private static IEnumerable<string> SplitAddresses(string addresses)
    {
        return addresses
            .Split([';', ','], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    private static void AddAddresses(MailAddressCollection collection, IEnumerable<string> addresses)
    {
        foreach (string address in addresses)
        {
            collection.Add(address);
        }
    }

    private static string BuildMessageId(string idempotencyKey, SmtpSenderProfileOptions senderProfile)
    {
        string domain = new MailAddress(senderProfile.FromEmail).Host;
        return $"<{idempotencyKey}@{domain}>";
    }
}
