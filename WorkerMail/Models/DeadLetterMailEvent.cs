namespace WorkerMail.Models;

public sealed class DeadLetterMailEvent
{
    public DateTimeOffset FailedAt { get; set; } = DateTimeOffset.UtcNow;
    public string Reason { get; set; } = string.Empty;
    public string ErrorMessage { get; set; } = string.Empty;
    public int AttemptCount { get; set; }
    public string SourceTopic { get; set; } = string.Empty;
    public int SourcePartition { get; set; }
    public long SourceOffset { get; set; }
    public string RawMessage { get; set; } = string.Empty;
    public MailEvent? MailEvent { get; set; }
}
