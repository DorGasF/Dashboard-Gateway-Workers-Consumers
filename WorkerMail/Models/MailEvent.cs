namespace WorkerMail.Models;

public sealed class MailEvent
{
    public Guid EventId { get; set; }
    public string? IdempotencyKey { get; set; }
    public string EventType { get; set; } = "email.send.requested";
    public DateTimeOffset OccurredAt { get; set; } = DateTimeOffset.UtcNow;
    public string Template { get; set; } = string.Empty;
    public string To { get; set; } = string.Empty;
    public List<string> Cc { get; set; } = [];
    public List<string> Bcc { get; set; } = [];
    public string? Subject { get; set; }
    public Dictionary<string, string> Payload { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public string ResolveIdempotencyKey()
    {
        if (!string.IsNullOrWhiteSpace(IdempotencyKey))
        {
            return IdempotencyKey.Trim();
        }

        return EventId != Guid.Empty ? EventId.ToString("N") : string.Empty;
    }
}
