using WorkerMail.Options;

namespace WorkerMail.Models;

public sealed class ResolvedMailDefinition
{
    public string MailType { get; init; } = string.Empty;
    public string Template { get; init; } = string.Empty;
    public string? SubjectOverride { get; init; }
    public string SenderProfileName { get; init; } = string.Empty;
    public SmtpSenderProfileOptions SenderProfile { get; init; } = null!;
}
