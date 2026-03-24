using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.ComponentModel.DataAnnotations;
using WorkerLogs;
using WorkerLogs.Options;
using WorkerLogs.Services;

const string GlobalMutexName = "Global\\RaimsWorkerLogsInstance";
using Mutex appMutex = new Mutex(initiallyOwned: false, name: GlobalMutexName, createdNew: out bool createdNew);

if (!createdNew)
{
    return;
}

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Configuration.Sources.Clear();
builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

builder.Services
    .AddOptions<ConsoleLoggingOptions>()
    .Bind(builder.Configuration.GetSection(ConsoleLoggingOptions.SectionName))
    .Validate(ValidateOptions, "Configuração inválida de log de console.")
    .ValidateOnStart();

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(ValidateOptions, "Configuração inválida de Kafka.")
    .ValidateOnStart();

builder.Services
    .AddOptions<WorkerOptions>()
    .Bind(builder.Configuration.GetSection(WorkerOptions.SectionName))
    .Validate(ValidateOptions, "Configuração inválida do Worker.")
    .ValidateOnStart();

builder.Services
    .AddOptions<StorageOptions>()
    .Bind(builder.Configuration.GetSection(StorageOptions.SectionName))
    .Validate(ValidateOptions, "Configuração inválida de armazenamento.")
    .ValidateOnStart();

ConsoleLoggingOptions consoleLoggingOptions = builder.Configuration
    .GetSection(ConsoleLoggingOptions.SectionName)
    .Get<ConsoleLoggingOptions>() ?? throw new InvalidOperationException("Seção ConsoleLogging não encontrada.");

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.TimestampFormat = consoleLoggingOptions.TimestampFormat;
    options.SingleLine = consoleLoggingOptions.SingleLine!.Value;
});

builder.Services.AddSingleton<IConsumer<string, string>>(serviceProvider =>
{
    KafkaOptions kafkaOptions = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaOptions>>().Value;
    return new ConsumerBuilder<string, string>(BuildConsumerConfig(kafkaOptions)).Build();
});

builder.Services.AddSingleton<IAdminClient>(serviceProvider =>
{
    KafkaOptions kafkaOptions = serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaOptions>>().Value;
    return new AdminClientBuilder(BuildAdminClientConfig(kafkaOptions)).Build();
});

builder.Services.AddSingleton<KafkaTopicProvisionerService>();
builder.Services.AddHostedService<Worker>();

await builder.Build().RunAsync();

static ConsumerConfig BuildConsumerConfig(KafkaOptions kafkaOptions)
{
    ConsumerConfig consumerConfig = new()
    {
        BootstrapServers = kafkaOptions.BootstrapServers,
        GroupId = kafkaOptions.GroupId,
        AutoOffsetReset = ParseAutoOffsetReset(kafkaOptions.AutoOffsetReset),
        EnableAutoCommit = kafkaOptions.EnableAutoCommit!.Value,
        AllowAutoCreateTopics = kafkaOptions.ConsumerAllowAutoCreateTopics!.Value,
        ClientId = kafkaOptions.ConsumerClientId,
        SessionTimeoutMs = kafkaOptions.SessionTimeoutMs!.Value,
        SocketTimeoutMs = kafkaOptions.SocketTimeoutMs!.Value
    };

    ApplyKafkaSecurity(consumerConfig, kafkaOptions);

    return consumerConfig;
}

static AdminClientConfig BuildAdminClientConfig(KafkaOptions kafkaOptions)
{
    AdminClientConfig adminClientConfig = new()
    {
        BootstrapServers = kafkaOptions.BootstrapServers,
        ClientId = kafkaOptions.AdminClientId,
        SocketTimeoutMs = kafkaOptions.SocketTimeoutMs!.Value
    };

    ApplyKafkaSecurity(adminClientConfig, kafkaOptions);

    return adminClientConfig;
}

static void ApplyKafkaSecurity(ClientConfig clientConfig, KafkaOptions kafkaOptions)
{
    if (!string.IsNullOrWhiteSpace(kafkaOptions.SecurityProtocol) &&
        Enum.TryParse(kafkaOptions.SecurityProtocol, ignoreCase: true, out SecurityProtocol securityProtocol))
    {
        clientConfig.SecurityProtocol = securityProtocol;
    }

    if (!string.IsNullOrWhiteSpace(kafkaOptions.SaslMechanism) &&
        Enum.TryParse(kafkaOptions.SaslMechanism, ignoreCase: true, out SaslMechanism saslMechanism))
    {
        clientConfig.SaslMechanism = saslMechanism;
    }

    if (!string.IsNullOrWhiteSpace(kafkaOptions.SaslUsername))
    {
        clientConfig.SaslUsername = kafkaOptions.SaslUsername;
    }

    if (!string.IsNullOrWhiteSpace(kafkaOptions.SaslPassword))
    {
        clientConfig.SaslPassword = kafkaOptions.SaslPassword;
    }
}

static AutoOffsetReset ParseAutoOffsetReset(string value)
{
    if (Enum.TryParse(value, ignoreCase: true, out AutoOffsetReset autoOffsetReset))
    {
        return autoOffsetReset;
    }

    throw new InvalidOperationException($"Valor inválido para Kafka:AutoOffsetReset: '{value}'.");
}

static bool ValidateOptions<TOptions>(TOptions options) where TOptions : class
{
    ValidationContext validationContext = new(options);
    return Validator.TryValidateObject(options, validationContext, null, validateAllProperties: true);
}
