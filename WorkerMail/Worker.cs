using Confluent.Kafka;
using System.Collections.Concurrent;
using WorkerMail.Models;
using WorkerMail.Options;
using WorkerMail.Services;

namespace WorkerMail;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly KafkaTopicProvisionerService _kafkaTopicProvisionerService;
    private readonly MailProcessingService _mailProcessingService;
    private readonly string _requestTopic;
    private readonly string _groupId;
    private readonly int _pollIntervalMs;
    private readonly int _idleDelayMs;
    private readonly int _retryDelayMs;
    private readonly int _shutdownDrainTimeoutSeconds;
    private readonly SemaphoreSlim _processingSemaphore;
    private readonly object _inFlightTasksLock = new();
    private readonly HashSet<Task> _inFlightTasks = [];
    private readonly KafkaPartitionTrackerService _partitionTrackerService;
    private readonly ConcurrentDictionary<TopicPartition, PartitionCommitState> _partitionStates = new();
    private readonly ConcurrentDictionary<TopicPartition, PendingCommitState> _pendingCommits = new();

    public Worker(
        ILogger<Worker> logger,
        IConsumer<string, string> consumer,
        KafkaPartitionTrackerService partitionTrackerService,
        KafkaTopicProvisionerService kafkaTopicProvisionerService,
        MailProcessingService mailProcessingService,
        Microsoft.Extensions.Options.IOptions<KafkaOptions> kafkaOptions,
        Microsoft.Extensions.Options.IOptions<WorkerOptions> workerOptions)
    {
        _logger = logger;
        _consumer = consumer;
        _partitionTrackerService = partitionTrackerService;
        _kafkaTopicProvisionerService = kafkaTopicProvisionerService;
        _mailProcessingService = mailProcessingService;
        _requestTopic = kafkaOptions.Value.RequestTopic;
        _groupId = kafkaOptions.Value.GroupId;
        _pollIntervalMs = workerOptions.Value.PollIntervalMs!.Value;
        _idleDelayMs = workerOptions.Value.IdleDelayMs!.Value;
        _retryDelayMs = workerOptions.Value.RetryDelayMs!.Value;
        _shutdownDrainTimeoutSeconds = workerOptions.Value.ShutdownDrainTimeoutSeconds!.Value;
        _processingSemaphore = new SemaphoreSlim(workerOptions.Value.MaxConcurrentMessages!.Value, workerOptions.Value.MaxConcurrentMessages!.Value);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            _logger.LogInformation(
                "WorkerMail iniciado. Tópico principal: {Topic}. Grupo Kafka: {GroupId}. Concorrência máxima: {MaxConcurrentMessages}",
                _requestTopic,
                _groupId,
                _processingSemaphore.CurrentCount);

            await _kafkaTopicProvisionerService.EnsureTopicsAvailableAsync(stoppingToken);
            _consumer.Subscribe(_requestTopic);

            while (!stoppingToken.IsCancellationRequested)
            {
                CommitPendingOffsets();

                if (_processingSemaphore.CurrentCount == 0)
                {
                    await Task.Delay(_idleDelayMs, stoppingToken);
                    continue;
                }

                try
                {
                    ConsumeResult<string, string>? consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(_pollIntervalMs));

                    if (consumeResult is null)
                    {
                        await Task.Delay(_idleDelayMs, stoppingToken);
                        continue;
                    }
                    await _processingSemaphore.WaitAsync(stoppingToken);

                    DispatchMessage(consumeResult, stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    if (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                    {
                        await Core.Log.EnqueueWarningAsync(
                            "Tópico Kafka ainda não disponível para o WorkerMail.",
                            ex,
                            new Dictionary<string, string>
                            {
                                ["worker"] = "WorkerMail",
                                ["groupId"] = _groupId,
                                ["topic"] = _requestTopic,
                                ["reason"] = "unknown_topic_or_partition"
                            });

                        _logger.LogWarning(
                            ex,
                            "O tópico Kafka {Topic} ainda não está disponível. O worker aguardará a criação/propagação do tópico.",
                            _requestTopic);

                        await _kafkaTopicProvisionerService.EnsureTopicsAvailableAsync(stoppingToken);
                    }
                    else
                    {
                        await Core.Log.EnqueueErrorAsync(
                            "Erro ao consumir mensagem do Kafka no WorkerMail.",
                            ex,
                            new Dictionary<string, string>
                            {
                                ["worker"] = "WorkerMail",
                                ["groupId"] = _groupId,
                                ["topic"] = _requestTopic
                            });

                        _logger.LogError(ex, "Erro ao consumir mensagem do Kafka");
                        await Task.Delay(_retryDelayMs, stoppingToken);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    await Core.Log.EnqueueErrorAsync(
                        "Erro inesperado no loop principal do WorkerMail.",
                        ex,
                        new Dictionary<string, string>
                        {
                            ["worker"] = "WorkerMail",
                            ["groupId"] = _groupId,
                            ["topic"] = _requestTopic
                        });

                    _logger.LogError(ex, "Erro inesperado no WorkerMail");
                    await Task.Delay(_retryDelayMs, stoppingToken);
                }
            }
        }
        finally
        {
            await DrainInFlightTasksAsync();
            CommitPendingOffsets();
            _consumer.Close();
            _consumer.Dispose();
        }
    }

    private void DispatchMessage(ConsumeResult<string, string> consumeResult, CancellationToken stoppingToken)
    {
        PartitionExecutionContext executionContext = _partitionTrackerService.GetExecutionContext(consumeResult.TopicPartition);
        EnsurePartitionStateInitialized(consumeResult.TopicPartition, consumeResult.Offset.Value, executionContext.Generation);

        CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, executionContext.CancellationToken);
        Task processingTask = ProcessMessageAsync(consumeResult, executionContext.Generation, linkedCts);

        lock (_inFlightTasksLock)
        {
            _inFlightTasks.Add(processingTask);
        }

        _ = processingTask.ContinueWith(_ =>
        {
            lock (_inFlightTasksLock)
            {
                _inFlightTasks.Remove(processingTask);
            }

            linkedCts.Dispose();
        }, TaskScheduler.Default);
    }

    private async Task ProcessMessageAsync(
        ConsumeResult<string, string> consumeResult,
        long generation,
        CancellationTokenSource linkedCts)
    {
        try
        {
            int attempt = 0;

            while (!linkedCts.IsCancellationRequested)
            {
                MailProcessingResult processingResult;
                attempt++;

                try
                {
                    processingResult = await _mailProcessingService.ProcessAsync(consumeResult, attempt, linkedCts.Token);
                }
                catch (OperationCanceledException) when (linkedCts.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    await Core.Log.EnqueueErrorAsync(
                        "Erro inesperado no processamento paralelo do WorkerMail.",
                        ex,
                        new Dictionary<string, string>
                        {
                            ["worker"] = "WorkerMail",
                            ["topic"] = consumeResult.Topic,
                            ["partition"] = consumeResult.Partition.Value.ToString(),
                            ["offset"] = consumeResult.Offset.Value.ToString()
                        });

                    _logger.LogError(ex, "Erro inesperado no processamento paralelo do WorkerMail");
                    processingResult = MailProcessingResult.Retry("Erro inesperado no processamento paralelo.");
                }

                if (processingResult.Action == MailProcessingAction.Commit)
                {
                    MarkOffsetAsCompleted(consumeResult.TopicPartition, consumeResult.Offset.Value, generation);
                    return;
                }

                _logger.LogInformation(
                    "Processamento solicitou retry. Partition={Partition} Offset={Offset} Generation={Generation} Attempt={Attempt} RetryDelayMs={RetryDelayMs} Reason={Reason}",
                    consumeResult.Partition.Value,
                    consumeResult.Offset.Value,
                    generation,
                    attempt,
                    _retryDelayMs,
                    processingResult.Reason);

                await Task.Delay(_retryDelayMs, linkedCts.Token);
            }
        }
        finally
        {
            _processingSemaphore.Release();
        }
    }

    private void EnsurePartitionStateInitialized(TopicPartition topicPartition, long offset, long generation)
    {
        PartitionCommitState state = _partitionStates.GetOrAdd(topicPartition, _ => new PartitionCommitState());

        lock (state.Sync)
        {
            if (!state.Initialized || state.Generation != generation)
            {
                state.Initialized = true;
                state.Generation = generation;
                state.NextCommitOffset = offset;
                state.CompletedOffsets.Clear();
            }
        }
    }

    private void MarkOffsetAsCompleted(TopicPartition topicPartition, long offset, long generation)
    {
        if (!_partitionTrackerService.IsGenerationCurrent(topicPartition, generation))
        {
            return;
        }

        PartitionCommitState state = _partitionStates.GetOrAdd(topicPartition, _ => new PartitionCommitState());
        long? nextCommitOffset = null;

        lock (state.Sync)
        {
            if (!state.Initialized || state.Generation != generation)
            {
                state.Initialized = true;
                state.Generation = generation;
                state.NextCommitOffset = offset;
                state.CompletedOffsets.Clear();
            }

            state.CompletedOffsets.Add(offset);

            bool advanced = false;
            while (state.CompletedOffsets.Contains(state.NextCommitOffset))
            {
                state.CompletedOffsets.Remove(state.NextCommitOffset);
                state.NextCommitOffset++;
                advanced = true;
            }

            if (advanced)
            {
                nextCommitOffset = state.NextCommitOffset;
            }
        }

        if (nextCommitOffset.HasValue)
        {
            _pendingCommits.AddOrUpdate(
                topicPartition,
                new PendingCommitState(generation, nextCommitOffset.Value),
                (_, existingState) => existingState.Generation == generation && existingState.Offset > nextCommitOffset.Value
                    ? existingState
                    : new PendingCommitState(generation, nextCommitOffset.Value));
        }
    }

    private void CommitPendingOffsets()
    {
        if (_pendingCommits.IsEmpty)
        {
            return;
        }

        List<TopicPartitionOffset> offsetsToCommit = [];

        foreach ((TopicPartition topicPartition, PendingCommitState nextCommitState) in _pendingCommits.ToArray())
        {
            if (_pendingCommits.TryRemove(topicPartition, out PendingCommitState resolvedState))
            {
                if (_partitionTrackerService.IsGenerationCurrent(topicPartition, resolvedState.Generation))
                {
                    offsetsToCommit.Add(new TopicPartitionOffset(topicPartition, new Offset(resolvedState.Offset)));
                }
            }
        }

        if (offsetsToCommit.Count == 0)
        {
            return;
        }

        try
        {
            _consumer.Commit(offsetsToCommit);
        }
        catch (Exception ex)
        {
            foreach (TopicPartitionOffset topicPartitionOffset in offsetsToCommit)
            {
                long generation = _partitionStates.TryGetValue(topicPartitionOffset.TopicPartition, out PartitionCommitState? state)
                    ? state.Generation
                    : 0;

                _pendingCommits.AddOrUpdate(
                    topicPartitionOffset.TopicPartition,
                    new PendingCommitState(generation, topicPartitionOffset.Offset.Value),
                    (_, existingState) => existingState.Offset > topicPartitionOffset.Offset.Value
                        ? existingState
                        : new PendingCommitState(generation, topicPartitionOffset.Offset.Value));
            }

            _logger.LogError(ex, "Erro ao confirmar offsets do WorkerMail");
        }
    }

    private async Task DrainInFlightTasksAsync()
    {
        Task[] tasks;

        lock (_inFlightTasksLock)
        {
            tasks = _inFlightTasks.ToArray();
        }

        if (tasks.Length == 0)
        {
            return;
        }

        _logger.LogInformation("Iniciando drenagem das tarefas em andamento. Quantidade={Count}", tasks.Length);
        Task allTasks = Task.WhenAll(tasks);
        Task timeoutTask = Task.Delay(TimeSpan.FromSeconds(_shutdownDrainTimeoutSeconds));
        Task completedTask = await Task.WhenAny(allTasks, timeoutTask);

        if (completedTask != allTasks)
        {
            _logger.LogWarning(
                "Timeout ao aguardar drenagem das tarefas em andamento do WorkerMail. Tarefas restantes: {Count}",
                tasks.Length);
        }
    }

    private sealed class PartitionCommitState
    {
        public object Sync { get; } = new();
        public bool Initialized { get; set; }
        public long Generation { get; set; }
        public long NextCommitOffset { get; set; }
        public HashSet<long> CompletedOffsets { get; } = [];
    }

    private readonly record struct PendingCommitState(long Generation, long Offset);
}
