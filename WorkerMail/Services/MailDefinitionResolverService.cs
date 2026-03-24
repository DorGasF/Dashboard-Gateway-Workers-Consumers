using Microsoft.Extensions.Options;
using WorkerMail.Models;
using WorkerMail.Options;

namespace WorkerMail.Services;

public sealed class MailDefinitionResolverService
{
    private readonly MailTypeOptions _mailTypeOptions;
    private readonly SmtpOptions _smtpOptions;

    public MailDefinitionResolverService(
        IOptions<MailTypeOptions> mailTypeOptions,
        IOptions<SmtpOptions> smtpOptions)
    {
        _mailTypeOptions = mailTypeOptions.Value;
        _smtpOptions = smtpOptions.Value;
    }

    public ResolvedMailDefinition Resolve(MailEvent mailEvent)
    {
        if (!string.IsNullOrWhiteSpace(mailEvent.MailType))
        {
            if (!_mailTypeOptions.Definitions.TryGetValue(mailEvent.MailType, out MailTypeDefinitionOptions? definition))
            {
                throw new InvalidOperationException($"MailType '{mailEvent.MailType}' não está configurado.");
            }

            if (string.IsNullOrWhiteSpace(definition.Template))
            {
                throw new InvalidOperationException($"MailType '{mailEvent.MailType}' não possui template configurado.");
            }

            SmtpSenderProfileOptions senderProfile = ResolveSenderProfile(definition.SenderProfile);

            return new ResolvedMailDefinition
            {
                MailType = mailEvent.MailType,
                Template = definition.Template,
                SubjectOverride = string.IsNullOrWhiteSpace(mailEvent.Subject) ? definition.Subject : mailEvent.Subject,
                SenderProfileName = string.IsNullOrWhiteSpace(definition.SenderProfile)
                    ? ResolveDefaultSenderProfileName()
                    : definition.SenderProfile,
                SenderProfile = senderProfile
            };
        }

        if (!string.IsNullOrWhiteSpace(mailEvent.Template))
        {
            string defaultSenderProfileName = ResolveDefaultSenderProfileName();

            return new ResolvedMailDefinition
            {
                MailType = "legacy.template",
                Template = mailEvent.Template,
                SubjectOverride = mailEvent.Subject,
                SenderProfileName = defaultSenderProfileName,
                SenderProfile = ResolveSenderProfile(defaultSenderProfileName)
            };
        }

        throw new InvalidOperationException("O evento não possui MailType nem Template.");
    }

    private SmtpSenderProfileOptions ResolveSenderProfile(string? profileName)
    {
        string resolvedProfileName = string.IsNullOrWhiteSpace(profileName)
            ? ResolveDefaultSenderProfileName()
            : profileName;

        if (!_smtpOptions.SenderProfiles.TryGetValue(resolvedProfileName, out SmtpSenderProfileOptions? senderProfile))
        {
            throw new InvalidOperationException($"SenderProfile '{resolvedProfileName}' não está configurado.");
        }

        return senderProfile;
    }

    private string ResolveDefaultSenderProfileName()
    {
        if (!string.IsNullOrWhiteSpace(_smtpOptions.DefaultSenderProfile))
        {
            return _smtpOptions.DefaultSenderProfile;
        }

        return _mailTypeOptions.DefaultSenderProfile;
    }
}
