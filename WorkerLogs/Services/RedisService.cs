using StackExchange.Redis;
using System.Net;
public class RedisService
{
    private readonly IConnectionMultiplexer _connection;
    private readonly IDatabase _db;
    private readonly ILogger<RedisService> _logger;
    private static bool _isWarmupDone = false;
    private static readonly object _lock = new();

    public RedisService(IConnectionMultiplexer connection, ILogger<RedisService> logger)
    {
        _connection = connection;
        _db = _connection.GetDatabase();
        _logger = logger;

        EnsureWarmup();
    }

    private void EnsureWarmup()
    {
        if (_isWarmupDone)
            return;

        lock (_lock)
        {
            if (_isWarmupDone)
                return;

            try
            {
                TimeSpan latency = _db.Ping();
                _logger.LogInformation("Redis pré-conectado ({Latency} ms)", latency.TotalMilliseconds);
                _isWarmupDone = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Falha no warmup do Redis");
            }
        }
    }

    public IDatabase GetDatabase() => _db;

    public IServer GetServer()
    {
        EndPoint endpoint = _connection.GetEndPoints().First();
        return _connection.GetServer(endpoint);
    }
}