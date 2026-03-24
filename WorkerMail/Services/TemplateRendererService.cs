using Microsoft.Extensions.Options;
using System.Text.RegularExpressions;
using WorkerMail.Models;
using WorkerMail.Options;

namespace WorkerMail.Services;

public sealed class TemplateRendererService
{
    private static readonly Regex PlaceholderRegex = new(@"\{\{\s*(?<key>[a-zA-Z0-9_.-]+)\s*\}\}", RegexOptions.Compiled);

    private readonly ILogger<TemplateRendererService> _logger;
    private readonly string _templateRootPath;
    private readonly IReadOnlyDictionary<string, CachedTemplate> _templateCache;

    public TemplateRendererService(IOptions<TemplateOptions> templateOptions, ILogger<TemplateRendererService> logger)
    {
        _logger = logger;
        _templateRootPath = Path.Combine(AppContext.BaseDirectory, templateOptions.Value.BasePath);
        _templateCache = LoadTemplateCache(_templateRootPath);

        _logger.LogInformation(
            "Cache de templates carregado na memória. Diretório={TemplateRootPath}. Quantidade={Count}",
            _templateRootPath,
            _templateCache.Count);
    }

    public Task<RenderedMail> RenderAsync(
        MailEvent mailEvent,
        string templateName,
        string? subjectOverride,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_templateCache.TryGetValue(templateName, out CachedTemplate? cachedTemplate))
        {
            throw new InvalidOperationException($"Template '{templateName}' não encontrado.");
        }

        string? bodyTemplate = cachedTemplate.HtmlBody ?? cachedTemplate.TextBody;
        if (string.IsNullOrWhiteSpace(bodyTemplate))
        {
            throw new InvalidOperationException($"Template '{templateName}' não encontrado.");
        }

        string? subjectTemplate = !string.IsNullOrWhiteSpace(subjectOverride)
            ? subjectOverride
            : cachedTemplate.Subject;

        if (string.IsNullOrWhiteSpace(subjectTemplate))
        {
            throw new InvalidOperationException($"Assunto do template '{templateName}' não encontrado.");
        }

        string subject = NormalizeSubject(ReplacePlaceholders(subjectTemplate, mailEvent));
        string body = ReplacePlaceholders(bodyTemplate, mailEvent);

        _logger.LogDebug("Template {Template} renderizado para {To}", templateName, mailEvent.To);

        return Task.FromResult(new RenderedMail
        {
            Subject = subject,
            Body = body,
            IsHtml = cachedTemplate.HtmlBody is not null
        });
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

    private IReadOnlyDictionary<string, CachedTemplate> LoadTemplateCache(string templateRootPath)
    {
        Dictionary<string, CachedTemplateBuilder> templates = new(StringComparer.OrdinalIgnoreCase);

        if (!Directory.Exists(templateRootPath))
        {
            _logger.LogWarning("Diretório de templates não encontrado durante o preload. Diretório={TemplateRootPath}", templateRootPath);
            return new Dictionary<string, CachedTemplate>(StringComparer.OrdinalIgnoreCase);
        }

        foreach (string filePath in Directory.EnumerateFiles(templateRootPath, "*", SearchOption.TopDirectoryOnly))
        {
            string fileName = Path.GetFileName(filePath);
            string content = File.ReadAllText(filePath);

            if (fileName.EndsWith(".subject.txt", StringComparison.OrdinalIgnoreCase))
            {
                string templateName = fileName[..^".subject.txt".Length];
                GetOrCreateTemplateBuilder(templates, templateName).Subject = content;
                continue;
            }

            if (fileName.EndsWith(".html", StringComparison.OrdinalIgnoreCase))
            {
                string templateName = fileName[..^".html".Length];
                GetOrCreateTemplateBuilder(templates, templateName).HtmlBody = content;
                continue;
            }

            if (fileName.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
            {
                string templateName = fileName[..^".txt".Length];
                GetOrCreateTemplateBuilder(templates, templateName).TextBody = content;
            }
        }

        return templates.ToDictionary(
            pair => pair.Key,
            pair => new CachedTemplate(pair.Value.HtmlBody, pair.Value.TextBody, pair.Value.Subject),
            StringComparer.OrdinalIgnoreCase);
    }

    private static CachedTemplateBuilder GetOrCreateTemplateBuilder(
        Dictionary<string, CachedTemplateBuilder> templates,
        string templateName)
    {
        if (!templates.TryGetValue(templateName, out CachedTemplateBuilder? builder))
        {
            builder = new CachedTemplateBuilder();
            templates[templateName] = builder;
        }

        return builder;
    }

    private sealed class CachedTemplateBuilder
    {
        public string? HtmlBody { get; set; }
        public string? TextBody { get; set; }
        public string? Subject { get; set; }
    }

    private sealed record CachedTemplate(string? HtmlBody, string? TextBody, string? Subject);
}
