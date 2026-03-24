namespace WorkerMail.Models;

public enum MailProcessingAction
{
    Commit = 1,
    Retry = 2
}

public sealed class MailProcessingResult
{
    public MailProcessingAction Action { get; }
    public string Reason { get; }

    private MailProcessingResult(MailProcessingAction action, string reason)
    {
        Action = action;
        Reason = reason;
    }

    public static MailProcessingResult Commit(string reason) => new(MailProcessingAction.Commit, reason);

    public static MailProcessingResult Retry(string reason) => new(MailProcessingAction.Retry, reason);
}
