using Microsoft.Extensions.Options;
using System.Text.RegularExpressions;
using WorkerMail.Models;
using WorkerMail.Options;

namespace WorkerMail.Services;

public sealed class TemplateRendererService
{
    private static readonly Regex PlaceholderRegex = new(@"\{\{\s*(?<key>[a-zA-Z0-9_.-]+)\s*\}\}", RegexOptions.Compiled);

    private readonly TemplateOptions _templateOptions;
    private readonly ILogger<TemplateRendererService> _logger;

    public TemplateRendererService(IOptions<TemplateOptions> templateOptions, ILogger<TemplateRendererService> logger)
    {
        _templateOptions = templateOptions.Value;
        _logger = logger;
    }

    public async Task<RenderedMail> RenderAsync(MailEvent mailEvent, CancellationToken cancellationToken)
    {
        string templateBasePath = Path.Combine(AppContext.BaseDirectory, _templateOptions.BasePath, mailEvent.Template);
        string htmlPath = templateBasePath + ".html";
        string textPath = templateBasePath + ".txt";
        string subjectPath = templateBasePath + ".subject.txt";

        if (!File.Exists(htmlPath) && !File.Exists(textPath))
        {
            throw new InvalidOperationException($"Template '{mailEvent.Template}' não encontrado.");
        }

        if (!File.Exists(subjectPath) && string.IsNullOrWhiteSpace(mailEvent.Subject))
        {
            throw new InvalidOperationException($"Assunto do template '{mailEvent.Template}' não encontrado.");
        }

        bool isHtml = File.Exists(htmlPath);
        string bodyTemplate = await File.ReadAllTextAsync(isHtml ? htmlPath : textPath, cancellationToken);
        string subjectTemplate = !string.IsNullOrWhiteSpace(mailEvent.Subject)
            ? mailEvent.Subject
            : await File.ReadAllTextAsync(subjectPath, cancellationToken);

        string subject = ReplacePlaceholders(subjectTemplate, mailEvent);
        string body = ReplacePlaceholders(bodyTemplate, mailEvent);

        _logger.LogDebug("Template {Template} renderizado para {To}", mailEvent.Template, mailEvent.To);

        return new RenderedMail
        {
            Subject = subject,
            Body = body,
            IsHtml = isHtml
        };
    }

    private static string ReplacePlaceholders(string template, MailEvent mailEvent)
    {
        Dictionary<string, string> values = new(mailEvent.Payload, StringComparer.OrdinalIgnoreCase)
        {
            ["eventId"] = mailEvent.EventId.ToString(),
            ["eventType"] = mailEvent.EventType,
            ["to"] = mailEvent.To,
            ["occurredAt"] = mailEvent.OccurredAt.ToString("O")
        };

        List<string> missingKeys = [];
        string rendered = PlaceholderRegex.Replace(template, match =>
        {
            string key = match.Groups["key"].Value;
            if (!values.TryGetValue(key, out string? value))
            {
                missingKeys.Add(key);
                return match.Value;
            }

            return value;
        });

        if (missingKeys.Count > 0)
        {
            throw new InvalidOperationException(
                $"Template possui placeholders sem valor: {string.Join(", ", missingKeys.Distinct(StringComparer.OrdinalIgnoreCase))}.");
        }

        return rendered;
    }
}
