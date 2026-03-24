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

    public async Task<RenderedMail> RenderAsync(
        MailEvent mailEvent,
        string templateName,
        string? subjectOverride,
        CancellationToken cancellationToken)
    {
        string templateBasePath = Path.Combine(AppContext.BaseDirectory, _templateOptions.BasePath, templateName);
        string htmlPath = templateBasePath + ".html";
        string textPath = templateBasePath + ".txt";
        string subjectPath = templateBasePath + ".subject.txt";

        if (!File.Exists(htmlPath) && !File.Exists(textPath))
        {
            throw new InvalidOperationException($"Template '{templateName}' não encontrado.");
        }

        if (!File.Exists(subjectPath) && string.IsNullOrWhiteSpace(subjectOverride))
        {
            throw new InvalidOperationException($"Assunto do template '{templateName}' não encontrado.");
        }

        bool isHtml = File.Exists(htmlPath);
        string bodyTemplate = await File.ReadAllTextAsync(isHtml ? htmlPath : textPath, cancellationToken);
        string subjectTemplate = !string.IsNullOrWhiteSpace(subjectOverride)
            ? subjectOverride
            : await File.ReadAllTextAsync(subjectPath, cancellationToken);

        string subject = NormalizeSubject(ReplacePlaceholders(subjectTemplate, mailEvent));
        string body = ReplacePlaceholders(bodyTemplate, mailEvent);

        _logger.LogDebug("Template {Template} renderizado para {To}", templateName, mailEvent.To);

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

    private static string NormalizeSubject(string subject)
    {
        string normalized = subject.Trim();

        if (string.IsNullOrWhiteSpace(normalized))
        {
            throw new InvalidOperationException("Assunto renderizado ficou vazio.");
        }

        if (normalized.Contains('\r') || normalized.Contains('\n'))
        {
            throw new InvalidOperationException("Assunto do e-mail contém quebra de linha inválida.");
        }

        return normalized;
    }
}
