using StackExchange.Redis;
using System.Net;
using System.Text.Json;
using WorkerMail.Models;

namespace WorkerMail.Services;

public sealed class RedisService
{
    private const string ReleaseLockScript = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
""";

    private readonly IConnectionMultiplexer _connection;
    private readonly IDatabase _database;
    private readonly ILogger<RedisService> _logger;
    private readonly string _lockKeyPrefix;
    private readonly string _attemptKeyPrefix;
    private readonly string _processedKeyPrefix;
    private readonly string _lastEventKeyPrefix;
    private static bool _isWarmupDone;
    private static readonly object LockObject = new();

    public RedisService(
        IConnectionMultiplexer connection,
        Microsoft.Extensions.Options.IOptions<WorkerMail.Options.RedisOptions> redisOptions,
        ILogger<RedisService> logger)
    {
        _connection = connection;
        _database = connection.GetDatabase();
        _logger = logger;
        _lockKeyPrefix = redisOptions.Value.LockKeyPrefix;
        _attemptKeyPrefix = redisOptions.Value.AttemptKeyPrefix;
        _processedKeyPrefix = redisOptions.Value.ProcessedKeyPrefix;
        _lastEventKeyPrefix = redisOptions.Value.LastEventKeyPrefix;

        EnsureWarmup();
    }

    public IDatabase GetDatabase() => _database;

    public IServer GetServer()
    {
        EndPoint endpoint = _connection.GetEndPoints().First();
        return _connection.GetServer(endpoint);
    }

    public Task<bool> IsProcessedAsync(string idempotencyKey)
    {
        return _database.KeyExistsAsync(BuildProcessedKey(idempotencyKey));
    }

    public Task<bool> TryAcquireProcessingLockAsync(string idempotencyKey, string owner, TimeSpan ttl)
    {
        return _database.StringSetAsync(BuildLockKey(idempotencyKey), owner, ttl, When.NotExists);
    }

    public async Task ReleaseProcessingLockAsync(string idempotencyKey, string owner)
    {
        await _database.ScriptEvaluateAsync(
            ReleaseLockScript,
            [BuildLockKey(idempotencyKey)],
            [owner]);
    }

    public async Task<int> IncrementAttemptAsync(string idempotencyKey, TimeSpan ttl)
    {
        RedisKey attemptKey = BuildAttemptKey(idempotencyKey);
        long attempt = await _database.StringIncrementAsync(attemptKey);
        await _database.KeyExpireAsync(attemptKey, ttl);
        return (int)attempt;
    }

    public Task ClearAttemptAsync(string idempotencyKey)
    {
        return _database.KeyDeleteAsync(BuildAttemptKey(idempotencyKey));
    }

    public Task MarkAsProcessedAsync(string idempotencyKey, object payload, TimeSpan ttl)
    {
        string serializedPayload = JsonSerializer.Serialize(payload);
        return _database.StringSetAsync(BuildProcessedKey(idempotencyKey), serializedPayload, ttl);
    }

    public Task CacheLastEventAsync(MailEvent mailEvent, TimeSpan ttl)
    {
        string key = $"{_lastEventKeyPrefix}{mailEvent.EventId}";
        string payload = JsonSerializer.Serialize(mailEvent);
        return _database.StringSetAsync(key, payload, ttl);
    }

    private void EnsureWarmup()
    {
        if (_isWarmupDone)
        {
            return;
        }

        lock (LockObject)
        {
            if (_isWarmupDone)
            {
                return;
            }

            try
            {
                TimeSpan latency = _database.Ping();
                _logger.LogInformation("Redis pré-conectado ({Latency} ms)", latency.TotalMilliseconds);
                _isWarmupDone = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Falha no warmup do Redis");
            }
        }
    }

    private RedisKey BuildLockKey(string idempotencyKey) => $"{_lockKeyPrefix}{idempotencyKey}";

    private RedisKey BuildAttemptKey(string idempotencyKey) => $"{_attemptKeyPrefix}{idempotencyKey}";

    private RedisKey BuildProcessedKey(string idempotencyKey) => $"{_processedKeyPrefix}{idempotencyKey}";
}
