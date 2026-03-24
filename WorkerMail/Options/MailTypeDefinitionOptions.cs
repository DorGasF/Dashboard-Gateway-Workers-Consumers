namespace WorkerMail.Options;

public sealed class MailTypeDefinitionOptions
{
    public string Template { get; set; } = string.Empty;
    public string SenderProfile { get; set; } = string.Empty;
    public string? Subject { get; set; }
}
