namespace WorkerMail.Models;

public sealed class RenderedMail
{
    public string Subject { get; init; } = string.Empty;
    public string Body { get; init; } = string.Empty;
    public bool IsHtml { get; init; }
}
