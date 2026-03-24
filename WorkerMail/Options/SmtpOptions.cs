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
    public string DefaultSenderProfile { get; set; } = null!;

    [Required]
    public Dictionary<string, SmtpSenderProfileOptions> SenderProfiles { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    [Required]
    [Range(1000, 120000)]
    public int? TimeoutMs { get; set; }
}
