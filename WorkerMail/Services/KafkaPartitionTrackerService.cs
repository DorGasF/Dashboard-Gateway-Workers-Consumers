using Confluent.Kafka;
using System.Collections.Concurrent;

namespace WorkerMail.Services;

public sealed class KafkaPartitionTrackerService
{
    private readonly ConcurrentDictionary<TopicPartition, PartitionRuntimeState> _states = new();

    public void HandleAssigned(IEnumerable<TopicPartition> partitions)
    {
        foreach (TopicPartition partition in partitions)
        {
            _states.AddOrUpdate(
                partition,
                _ => new PartitionRuntimeState(),
                (_, current) =>
                {
                    current.RevokeCurrentGeneration();
                    current.StartNewGeneration();
                    return current;
                });
        }
    }

    public void HandleRevoked(IEnumerable<TopicPartitionOffset> partitions)
    {
        foreach (TopicPartitionOffset partition in partitions)
        {
            if (_states.TryGetValue(partition.TopicPartition, out PartitionRuntimeState? state))
            {
                state.RevokeCurrentGeneration();
            }
        }
    }

    public PartitionExecutionContext GetExecutionContext(TopicPartition partition)
    {
        PartitionRuntimeState state = _states.GetOrAdd(partition, _ => new PartitionRuntimeState());
        return state.GetExecutionContext();
    }

    public bool IsGenerationCurrent(TopicPartition partition, long generation)
    {
        return _states.TryGetValue(partition, out PartitionRuntimeState? state) &&
               state.IsGenerationCurrent(generation);
    }

    private sealed class PartitionRuntimeState
    {
        private readonly object _sync = new();
        private long _generation = 1;
        private CancellationTokenSource _cts = new();
        private bool _revoked;

        public PartitionRuntimeState()
        {
        }

        public void StartNewGeneration()
        {
            lock (_sync)
            {
                _generation++;
                _cts.Dispose();
                _cts = new CancellationTokenSource();
                _revoked = false;
            }
        }

        public void RevokeCurrentGeneration()
        {
            lock (_sync)
            {
                if (_revoked)
                {
                    return;
                }

                _revoked = true;
                _cts.Cancel();
            }
        }

        public PartitionExecutionContext GetExecutionContext()
        {
            lock (_sync)
            {
                return new PartitionExecutionContext(_generation, _cts.Token, _revoked);
            }
        }

        public bool IsGenerationCurrent(long generation)
        {
            lock (_sync)
            {
                return !_revoked && _generation == generation;
            }
        }
    }
}

public readonly record struct PartitionExecutionContext(long Generation, CancellationToken CancellationToken, bool Revoked);
