using StackExchange.Redis;
using WorkerLogs;

const string GlobalMutexName = "Global\\RaimsWorkerLogsInstance";
using Mutex appMutex = new Mutex(initiallyOwned: false, name: GlobalMutexName, createdNew: out bool createdNew);

if (!createdNew)
{
    return;
}

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder.Configuration.SetBasePath(AppContext.BaseDirectory).AddJsonFile("appsettings.json", optional: false, reloadOnChange: true).AddEnvironmentVariables();

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.TimestampFormat = "[HH:mm:ss] ";
    options.SingleLine = true;
});

IConfigurationSection redisConfig = builder.Configuration.GetSection("Redis");
string redisHost = redisConfig.GetValue<string>("Host") ?? "localhost";
int redisPort = redisConfig.GetValue<int>("Port", 6379);

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    ConfigurationOptions options = ConfigurationOptions.Parse($"{redisHost}:{redisPort}");
    options.AbortOnConnectFail = false;
    options.ReconnectRetryPolicy = new ExponentialRetry(5000);
    options.ConnectRetry = 100;
    options.ConnectTimeout = 5000;
    options.KeepAlive = 5;
    options.AsyncTimeout = 5000;
    options.AllowAdmin = false;
    options.ClientName = "WorkerLogs";

    return ConnectionMultiplexer.Connect(options);
});

builder.Services.AddSingleton<RedisService>();
builder.Services.AddHostedService<Worker>();

await builder.Build().RunAsync();