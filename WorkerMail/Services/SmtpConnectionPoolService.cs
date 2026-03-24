using Microsoft.Extensions.Options;
using System.Net;
using System.Net.Mail;
using WorkerMail.Options;

namespace WorkerMail.Services;

public sealed class SmtpConnectionPoolService : IDisposable
{
    private readonly SmtpOptions _smtpOptions;
    private readonly bool _developmentMode;
    private readonly GlobalConnectionPoolState _state;
    private int _disposed;

    public SmtpConnectionPoolService(
        IOptions<SmtpOptions> smtpOptions,
        WorkerRuntimeContext runtimeContext)
    {
        _smtpOptions = smtpOptions.Value;
        _developmentMode = runtimeContext.DevelopmentMode;
        _state = new GlobalConnectionPoolState(
            _smtpOptions.MaxConnections!.Value,
            _smtpOptions.MaxMessagesPerConnection!.Value);
    }

    public async Task<SmtpConnectionLease> AcquireAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (_state.Sync)
            {
                if (_state.AvailableClients.Count > 0)
                {
                    PooledSmtpClient pooledClient = _state.AvailableClients.Dequeue();
                    return new SmtpConnectionLease(_state, pooledClient);
                }

                if (_state.CreatedClients < _state.MaxConnections)
                {
                    _state.CreatedClients++;
                    PooledSmtpClient pooledClient = CreatePooledClient();
                    return new SmtpConnectionLease(_state, pooledClient);
                }
            }

            await _state.AvailableSignal.WaitAsync(cancellationToken);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        lock (_state.Sync)
        {
            while (_state.AvailableClients.Count > 0)
            {
                PooledSmtpClient pooledClient = _state.AvailableClients.Dequeue();
                pooledClient.Dispose();
            }
        }

        _state.AvailableSignal.Dispose();
    }

    private PooledSmtpClient CreatePooledClient()
    {
        if (_developmentMode)
        {
            return new PooledSmtpClient(null);
        }

        SmtpClient smtpClient = new(_smtpOptions.Host, _smtpOptions.Port!.Value)
        {
            EnableSsl = _smtpOptions.EnableSsl!.Value,
            UseDefaultCredentials = _smtpOptions.UseDefaultCredentials!.Value,
            DeliveryMethod = SmtpDeliveryMethod.Network,
            Timeout = _smtpOptions.TimeoutMs!.Value
        };

        if (!_smtpOptions.UseDefaultCredentials.Value && !string.IsNullOrWhiteSpace(_smtpOptions.Username))
        {
            smtpClient.Credentials = new NetworkCredential(_smtpOptions.Username, _smtpOptions.Password);
        }

        return new PooledSmtpClient(smtpClient);
    }

    internal sealed class GlobalConnectionPoolState
    {
        public GlobalConnectionPoolState(int maxConnections, int maxMessagesPerConnection)
        {
            MaxConnections = maxConnections;
            MaxMessagesPerConnection = maxMessagesPerConnection;
            AvailableSignal = new SemaphoreSlim(0);
        }

        public object Sync { get; } = new();
        public int MaxConnections { get; }
        public int MaxMessagesPerConnection { get; }
        public int CreatedClients { get; set; }
        public Queue<PooledSmtpClient> AvailableClients { get; } = new();
        public SemaphoreSlim AvailableSignal { get; }
    }

    internal sealed class PooledSmtpClient : IDisposable
    {
        public PooledSmtpClient(SmtpClient? client)
        {
            Client = client;
        }

        public SmtpClient? Client { get; private set; }
        public int MessagesSent { get; set; }

        public void Dispose()
        {
            Client?.Dispose();
            Client = null;
        }
    }

    public sealed class SmtpConnectionLease : IAsyncDisposable, IDisposable
    {
        private readonly GlobalConnectionPoolState _state;
        private readonly PooledSmtpClient _pooledClient;
        private int _released;
        private int _broken;

        internal SmtpConnectionLease(GlobalConnectionPoolState state, PooledSmtpClient pooledClient)
        {
            _state = state;
            _pooledClient = pooledClient;
        }

        public SmtpClient? Client => _pooledClient.Client;

        public void MarkBroken()
        {
            Interlocked.Exchange(ref _broken, 1);
        }

        public void MarkMessageSent()
        {
            _pooledClient.MessagesSent++;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 1)
            {
                return;
            }

            lock (_state.Sync)
            {
                if (Volatile.Read(ref _broken) == 1 || _pooledClient.MessagesSent >= _state.MaxMessagesPerConnection)
                {
                    _pooledClient.Dispose();
                    _state.CreatedClients--;
                    _state.AvailableSignal.Release();
                    return;
                }

                _state.AvailableClients.Enqueue(_pooledClient);
                _state.AvailableSignal.Release();
            }
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
