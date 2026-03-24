namespace WorkerMail.Models;

public sealed class CoreLogEntry
{
    public Guid LogId { get; set; }
    public string Level { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public string? Exception { get; set; }
    public string? ExceptionType { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    public string ApplicationName { get; set; } = string.Empty;
    public string SourceType { get; set; } = string.Empty;
    public string MachineName { get; set; } = string.Empty;
    public int ProcessId { get; set; }
    public string MemberName { get; set; } = string.Empty;
    public string FilePath { get; set; } = string.Empty;
    public int LineNumber { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}
