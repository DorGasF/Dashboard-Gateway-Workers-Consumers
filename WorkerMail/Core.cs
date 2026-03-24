using WorkerMail.Services;

namespace WorkerMail;

public static class Core
{
    public static LogQueue Log { get; private set; } = null!;

    public static void Initialize(LogQueue logQueue)
    {
        Log = logQueue;
    }
}
