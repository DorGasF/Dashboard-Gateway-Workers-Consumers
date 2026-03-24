using System.ComponentModel.DataAnnotations;

namespace WorkerMail.Options;

public sealed class SmtpOptions
{
    public const string SectionName = "Smtp";

    [Required]
    public string Host { get; set; } = null!;

    [Required]
    [Range(1, 65535)]
    public int? Port { get; set; }

    public string? Username { get; set; }
    public string? Password { get; set; }

    [Required]
    public bool? EnableSsl { get; set; }

    [Required]
    public bool? UseDefaultCredentials { get; set; }

    [Required]
    [EmailAddress]
    public string FromEmail { get; set; } = null!;

    [Required]
    public string FromName { get; set; } = null!;

    [EmailAddress]
    public string? ReplyToEmail { get; set; }

    public string? ReplyToName { get; set; }

    [Required]
    [Range(1000, 120000)]
    public int? TimeoutMs { get; set; }
}
