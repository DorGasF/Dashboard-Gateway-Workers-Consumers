using Confluent.Kafka;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.ComponentModel.DataAnnotations;
using WorkerMail;
using WorkerMail.Options;
using WorkerMail.Services;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Configuration.Sources.Clear();
builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

builder.Services
    .AddOptions<RedisOptions>()
    .Bind(builder.Configuration.GetSection(RedisOptions.SectionName))
    .Validate(ValidateOptions, "Configuração inválida de Redis.")
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
    .AddOptions<SmtpOptions>()
    .Bind(builder.Configuration.GetSection(SmtpOptions.SectionName))
    .Validate(ValidateOptions, "Configuração inválida de SMTP.")
    .ValidateOnStart();

builder.Services
    .AddOptions<TemplateOptions>()
    .Bind(builder.Configuration.GetSection(TemplateOptions.SectionName))
    .Validate(ValidateOptions, "Configuração inválida de templates.")
    .ValidateOnStart();

builder.Services
    .AddOptions<ConsoleLoggingOptions>()
    .Bind(builder.Configuration.GetSection(ConsoleLoggingOptions.SectionName))
    .Validate(ValidateOptions, "Configuração inválida de log de console.")
    .ValidateOnStart();

builder.Services
    .AddOptions<CoreLogOptions>()
    .Bind(builder.Configuration.GetSection(CoreLogOptions.SectionName))
    .Validate(ValidateOptions, "Configuração inválida do CoreLog.")
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

builder.Services.AddSingleton<IConnectionMultiplexer>(serviceProvider =>
{
    RedisOptions redisOptions = serviceProvider.GetRequiredService<IOptions<RedisOptions>>().Value;

    ConfigurationOptions options = new()
    {
        AbortOnConnectFail = redisOptions.AbortOnConnectFail!.Value,
        ReconnectRetryPolicy = new ExponentialRetry(redisOptions.ReconnectRetryDelayMs!.Value),
        ConnectRetry = redisOptions.ConnectRetry!.Value,
        ConnectTimeout = redisOptions.ConnectTimeoutMs!.Value,
        KeepAlive = redisOptions.KeepAliveSeconds!.Value,
        AsyncTimeout = redisOptions.AsyncTimeoutMs!.Value,
        AllowAdmin = redisOptions.AllowAdmin!.Value,
        ClientName = redisOptions.ClientName,
        DefaultDatabase = redisOptions.Database!.Value
    };

    options.EndPoints.Add(redisOptions.Host, redisOptions.Port!.Value);

    if (!string.IsNullOrWhiteSpace(redisOptions.Password))
    {
        options.Password = redisOptions.Password;
    }

    return ConnectionMultiplexer.Connect(options);
});

builder.Services.AddSingleton<IConsumer<string, string>>(serviceProvider =>
{
    KafkaOptions kafkaOptions = serviceProvider.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new ConsumerBuilder<string, string>(BuildConsumerConfig(kafkaOptions)).Build();
});

builder.Services.AddSingleton<IProducer<string, string>>(serviceProvider =>
{
    KafkaOptions kafkaOptions = serviceProvider.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new ProducerBuilder<string, string>(BuildProducerConfig(kafkaOptions)).Build();
});

builder.Services.AddSingleton<IAdminClient>(serviceProvider =>
{
    KafkaOptions kafkaOptions = serviceProvider.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new AdminClientBuilder(BuildAdminClientConfig(kafkaOptions)).Build();
});

builder.Services.AddSingleton<RedisService>();
builder.Services.AddSingleton<KafkaTopicProvisionerService>();
builder.Services.AddSingleton<LogQueue>();
builder.Services.AddSingleton<TemplateRendererService>();
builder.Services.AddSingleton<SmtpEmailSender>();
builder.Services.AddSingleton<MailProcessingService>();
builder.Services.AddHostedService<Worker>();

IHost host = builder.Build();
LogQueue coreLogQueue = host.Services.GetRequiredService<LogQueue>();
Core.Initialize(coreLogQueue);
await host.RunAsync();

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

static ProducerConfig BuildProducerConfig(KafkaOptions kafkaOptions)
{
    ProducerConfig producerConfig = new()
    {
        BootstrapServers = kafkaOptions.BootstrapServers,
        AllowAutoCreateTopics = kafkaOptions.ProducerAllowAutoCreateTopics!.Value,
        ClientId = kafkaOptions.ProducerClientId,
        SocketTimeoutMs = kafkaOptions.SocketTimeoutMs!.Value
    };

    ApplyKafkaSecurity(producerConfig, kafkaOptions);

    return producerConfig;
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
