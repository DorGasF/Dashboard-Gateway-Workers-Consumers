using StackExchange.Redis;
using System.Text;
using System.Text.Json;

namespace WorkerLogs
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly RedisService _redisService;
        private readonly int _batchSize;
        private readonly int _flushInterval;
        private readonly List<string> _buffer = new();
        private readonly object _lock = new();
        private static int _isFlushing = 0;

        public Worker(ILogger<Worker> logger, RedisService redisService, IConfiguration config)
        {
            _logger = logger;
            _redisService = redisService;
            _batchSize = config.GetSection("Worker").GetValue<int>("BatchSize", 10000);
            _flushInterval = config.GetSection("Worker").GetValue<int>("FlushIntervalMs", 1000);
            Directory.CreateDirectory(Path.Combine(AppContext.BaseDirectory, "Logs"));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("WorkerLogs iniciado com batch de {batchSize} e flush {flush}ms", _batchSize, _flushInterval);

            IDatabase db = _redisService.GetDatabase();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    RedisValue[] entries = await db.ListLeftPopAsync("application:logs", _batchSize);

                    if (entries.Length == 0)
                    {
                        _ = Task.Run(FlushToDiskAsync);
                        await Task.Delay(_flushInterval, stoppingToken);
                        continue;
                    }

                    int processed = 0;
                    foreach (RedisValue entry in entries)
                    {
                        try
                        {
                            if (entry.IsNullOrEmpty) continue;

                            LogEntry? log = JsonSerializer.Deserialize<LogEntry>(entry.ToString());
                            if (log == null) continue;

                            string line = $"[{DateTime.UtcNow:O}] [{log.level}] {log.message}";
                            if (!string.IsNullOrEmpty(log.exception))
                                line += $" | EX: {log.exception}";

                            lock (_lock)
                                _buffer.Add(line);

                            processed++;
                        }
                        catch { }
                    }

                    if (processed > 0)
                        _logger.LogInformation("Processados {count} logs do Redis", processed);

                    if (_buffer.Count >= _batchSize)
                        _ = Task.Run(FlushToDiskAsync);
                }
                catch (RedisConnectionException)
                {
                    _logger.LogWarning("Conexão Redis perdida, tentando reconectar...");
                    await Task.Delay(2000, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro inesperado ao processar logs");
                    await Task.Delay(2000, stoppingToken);
                }
            }
        }

        private async Task FlushToDiskAsync()
        {
            if (Interlocked.Exchange(ref _isFlushing, 1) == 1)
                return;

            List<string> lines;
            lock (_lock)
            {
                if (_buffer.Count == 0)
                {
                    Interlocked.Exchange(ref _isFlushing, 0);
                    return;
                }

                lines = new List<string>(_buffer);
                _buffer.Clear();
            }

            try
            {
                string filePath = Path.Combine(AppContext.BaseDirectory, "Logs", $"{DateTime.UtcNow:yyyy-MM-dd}.log");
                await File.AppendAllLinesAsync(filePath, lines, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Erro ao gravar logs em disco");
            }
            finally
            {
                Interlocked.Exchange(ref _isFlushing, 0);
            }
        }

        private class LogEntry
        {
            public string? level { get; set; }
            public string? message { get; set; }
            public string? exception { get; set; }
        }
    }
}