using System.ComponentModel.DataAnnotations;

namespace WorkerMail.Options;

public sealed class SmtpSenderProfileOptions
{
    [Required]
    [EmailAddress]
    public string FromEmail { get; set; } = null!;

    [Required]
    public string FromName { get; set; } = null!;

    [EmailAddress]
    public string? ReplyToEmail { get; set; }

    public string? ReplyToName { get; set; }
}
