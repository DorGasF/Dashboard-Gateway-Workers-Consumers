using Microsoft.Extensions.Options;
using WorkerMail.Options;

namespace WorkerMail.Services;

public sealed class WorkerRuntimeContext
{
    public WorkerRuntimeContext(IOptions<SmtpOptions> smtpOptions)
    {
        DevelopmentMode = smtpOptions.Value.DevelopmentMode!.Value;
    }

    public bool DevelopmentMode { get; }
}
