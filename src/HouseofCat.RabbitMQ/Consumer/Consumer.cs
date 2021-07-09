using HouseofCat.Logger;
using HouseofCat.RabbitMQ.Pools;
using HouseofCat.Utilities.Errors;
using HouseofCat.Dataflows;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace HouseofCat.RabbitMQ
{
    public interface IConsumer<TFromQueue>
    {
        IChannelPool ChannelPool { get; }
        RabbitOptions Options { get; }
        ConsumerOptions ConsumerOptions { get; }
        bool Started { get; }

        Task DataflowExecutionEngineAsync(Func<TFromQueue, Task<bool>> workBodyAsync, int maxDoP = 4, bool ensureOrdered = true, CancellationToken token = default);

        ChannelReader<TFromQueue> GetConsumerBuffer();
        ValueTask<TFromQueue> ReadAsync();
        Task<IEnumerable<TFromQueue>> ReadUntilEmptyAsync();
        Task StartConsumerAsync();
        Task StopConsumerAsync(bool immediate = false);
        IAsyncEnumerable<TFromQueue> StreamOutUntilClosedAsync();
        IAsyncEnumerable<TFromQueue> StreamOutUntilEmptyAsync();
    }

    public class Consumer : IConsumer<ReceivedData>, IDisposable
    {
        private readonly ILogger<Consumer> _logger;
        private readonly SemaphoreSlim _conLock = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _dataFlowExecLock = new SemaphoreSlim(1, 1);
        private IChannelHost _chanHost;
        private bool _disposedValue;
        private Channel<ReceivedData> _dataBuffer;
        private bool _shutdown;

        public RabbitOptions Options { get; }
        public ConsumerOptions ConsumerOptions { get; }

        public IChannelPool ChannelPool { get; }
        public bool Started { get; private set; }

        public Consumer(RabbitOptions options, string consumerName)
            : this(new ChannelPool(options), consumerName)
        { }

        public Consumer(IChannelPool channelPool, string consumerName)
            : this(
                  channelPool,
                  channelPool.Options.GetConsumerOptions(consumerName))
        {
            Guard.AgainstNull(channelPool, nameof(channelPool));
            Guard.AgainstNullOrEmpty(consumerName, nameof(consumerName));
        }

        public Consumer(IChannelPool channelPool, ConsumerOptions consumerOptions)
        {
            Guard.AgainstNull(channelPool, nameof(channelPool));
            Guard.AgainstNull(consumerOptions, nameof(consumerOptions));

            _logger = LogHelper.GetLogger<Consumer>();
            Options = channelPool.Options;
            ChannelPool = channelPool;
            ConsumerOptions = consumerOptions;
        }

        public async Task StartConsumerAsync()
        {
            await _conLock
                .WaitAsync()
                .ConfigureAwait(false);

            try
            {
                if (!Started && ConsumerOptions.Enabled)
                {
                    await SetChannelHostAsync().ConfigureAwait(false);
                    _shutdown = false;
                    _dataBuffer = Channel.CreateBounded<ReceivedData>(
                        new BoundedChannelOptions(ConsumerOptions.BatchSize.Value)
                        {
                            FullMode = ConsumerOptions.BehaviorWhenFull.Value
                        });

                    await Task.Yield();
                    bool success;
                    do
                    {
                        _logger.LogTrace(LogMessages.Consumers.StartingConsumerLoop, ConsumerOptions.ConsumerName);
                        success = await StartConsumingAsync().ConfigureAwait(false);
                    }
                    while (!success);

                    _logger.LogDebug(LogMessages.Consumers.Started, ConsumerOptions.ConsumerName);

                    Started = true;
                }
            }
            finally { _conLock.Release(); }
        }

        public async Task StopConsumerAsync(bool immediate = false)
        {
            await _conLock
                .WaitAsync()
                .ConfigureAwait(false);

            _logger.LogDebug(LogMessages.Consumers.StopConsumer, ConsumerOptions.ConsumerName);

            try
            {
                if (Started)
                {
                    _shutdown = true;
                    _dataBuffer.Writer.Complete();

                    if (immediate)
                    {
                        await _chanHost.CloseAsync().ConfigureAwait(false);
                    }

                    await _dataBuffer
                        .Reader
                        .Completion
                        .ConfigureAwait(false);

                    Started = false;
                    _logger.LogDebug(
                        LogMessages.Consumers.StoppedConsumer,
                        ConsumerOptions.ConsumerName);
                }
            }
            finally { _conLock.Release(); }
        }

        private async Task<bool> StartConsumingAsync()
        {
            if (_shutdown)
            { return false; }

            _logger.LogInformation(
                LogMessages.Consumers.StartingConsumer,
                ConsumerOptions.ConsumerName);

            if (Options.FactoryOptions.EnableDispatchConsumersAsync)
            {
                if (_asyncConsumer != null) // Cleanup operation, this prevents an EventHandler leak.
                {
                    _asyncConsumer.Received -= ReceiveHandlerAsync;
                    _asyncConsumer.Shutdown -= ConsumerShutdownAsync;
                }

                try
                {
                    _asyncConsumer = await CreateAsyncConsumer().ConfigureAwait(false);
                    if (_asyncConsumer == null) { return false; }

                    var channel = await _chanHost.GetChannelAsync().ConfigureAwait(false);
                    
                    channel
                        .BasicConsume(
                            ConsumerOptions.QueueName,
                            ConsumerOptions.AutoAck ?? false,
                            ConsumerOptions.ConsumerName,
                            ConsumerOptions.NoLocal ?? false,
                            ConsumerOptions.Exclusive ?? false,
                            null,
                            _asyncConsumer);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Exception creating internal RabbitMQ consumer. Retrying...");
                    await Task.Delay(1000).ConfigureAwait(false);
                    await _chanHost.MakeChannelAsync().ConfigureAwait(false);
                    return false;
                }
            }
            else
            {
                if (_consumer != null) // Cleanup operation, this prevents an EventHandler leak.
                {
                    _consumer.Received -= ReceiveHandler;
                    _consumer.Shutdown -= ConsumerShutdown;
                }

                try
                {
                    _consumer = await CreateConsumer().ConfigureAwait(false);
                    if (_consumer == null) { return false; }

                    
                    var channel = await _chanHost.GetChannelAsync().ConfigureAwait(false);
                    channel
                        .BasicConsume(
                            ConsumerOptions.QueueName,
                            ConsumerOptions.AutoAck ?? false,
                            ConsumerOptions.ConsumerName,
                            ConsumerOptions.NoLocal ?? false,
                            ConsumerOptions.Exclusive ?? false,
                            null,
                            _consumer);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Exception creating internal RabbitMQ consumer. Retrying...");
                    await Task.Delay(1000).ConfigureAwait(false);
                    await _chanHost.MakeChannelAsync().ConfigureAwait(false);
                    return false;
                }
            }

            _logger.LogInformation(
                LogMessages.Consumers.StartedConsumer,
                ConsumerOptions.ConsumerName);

            return true;
        }

        private AsyncEventingBasicConsumer _asyncConsumer;
        private EventingBasicConsumer _consumer;

        private async Task SetChannelHostAsync()
        {
            if (ConsumerOptions.UseTransientChannels ?? true)
            {
                var autoAck = ConsumerOptions.AutoAck ?? false;
                _logger.LogTrace(LogMessages.Consumers.GettingTransientChannelHost, ConsumerOptions.ConsumerName);
                _chanHost = await ChannelPool
                    .GetTransientChannelAsync(!autoAck)
                    .ConfigureAwait(false);
            }
            else if (ConsumerOptions.AutoAck ?? false)
            {
                _logger.LogTrace(LogMessages.Consumers.GettingChannelHost, ConsumerOptions.ConsumerName);
                _chanHost = await ChannelPool
                    .GetChannelAsync()
                    .ConfigureAwait(false);
            }
            else
            {
                _logger.LogTrace(LogMessages.Consumers.GettingAckChannelHost, ConsumerOptions.ConsumerName);
                _chanHost = await ChannelPool
                    .GetAckChannelAsync()
                    .ConfigureAwait(false);
            }

            _logger.LogDebug(
                LogMessages.Consumers.ChannelEstablished,
                ConsumerOptions.ConsumerName,
                _chanHost?.ChannelId ?? 0ul);
        }

        private async Task<EventingBasicConsumer> CreateConsumer()
        {
            EventingBasicConsumer consumer = null;

            var channel = await _chanHost.GetChannelAsync().ConfigureAwait(false);
            channel.BasicQos(0, ConsumerOptions.BatchSize.Value, false);
            consumer = new EventingBasicConsumer(channel);

            consumer.Received += ReceiveHandler;
            consumer.Shutdown += ConsumerShutdown;

            return consumer;
        }

        private async void ReceiveHandler(object _, BasicDeliverEventArgs bdea)
        {
            var rabbitMessage = new ReceivedData(
                await _chanHost.GetChannelAsync().ConfigureAwait(false), bdea, !(ConsumerOptions.AutoAck ?? false));

            _logger.LogDebug(
                LogMessages.Consumers.ConsumerMessageReceived,
                ConsumerOptions.ConsumerName,
                bdea.DeliveryTag);

            if (await _dataBuffer
                    .Writer
                    .WaitToWriteAsync()
                    .ConfigureAwait(false))
            {
                await _dataBuffer
                    .Writer
                    .WriteAsync(rabbitMessage);
            }
        }

        private async void ConsumerShutdown(object sender, ShutdownEventArgs e)
        {
            await HandleUnexpectedShutdownAsync(e)
                .ConfigureAwait(false);
        }

        private async Task<AsyncEventingBasicConsumer> CreateAsyncConsumer()
        {
            AsyncEventingBasicConsumer consumer = null;

            var channel = await _chanHost.GetChannelAsync().ConfigureAwait(false);
            channel.BasicQos(0, ConsumerOptions.BatchSize.Value, false);
            consumer = new AsyncEventingBasicConsumer(channel);

            consumer.Received += ReceiveHandlerAsync;
            consumer.Shutdown += ConsumerShutdownAsync;

            return consumer;
        }

        private async Task ReceiveHandlerAsync(object o, BasicDeliverEventArgs bdea)
        {
            var rabbitMessage = new ReceivedData(
                await _chanHost.GetChannelAsync().ConfigureAwait(false), bdea, !(ConsumerOptions.AutoAck ?? false));

            _logger.LogDebug(
                LogMessages.Consumers.ConsumerAsyncMessageReceived,
                ConsumerOptions.ConsumerName,
                bdea.DeliveryTag);

            if (await _dataBuffer
                .Writer
                .WaitToWriteAsync()
                .ConfigureAwait(false))
            {
                await _dataBuffer
                    .Writer
                    .WriteAsync(rabbitMessage);
            }
        }

        private async Task ConsumerShutdownAsync(object sender, ShutdownEventArgs e)
        {
            await HandleUnexpectedShutdownAsync(e)
                .ConfigureAwait(false);
        }

        private async Task HandleUnexpectedShutdownAsync(ShutdownEventArgs e)
        {
            if (!_shutdown)
            {
                await Task.Yield();
                bool success;
                do
                {
                    success = await _chanHost.MakeChannelAsync().ConfigureAwait(false);

                    if (success)
                    {
                        _logger.LogWarning(
                            LogMessages.Consumers.ConsumerShutdownEvent,
                            ConsumerOptions.ConsumerName,
                            e.ReplyText);

                        success = await StartConsumingAsync().ConfigureAwait(false);
                    }
                }
                while (!_shutdown && !success);
            }
        }

        public ChannelReader<ReceivedData> GetConsumerBuffer() => _dataBuffer.Reader;

        public async ValueTask<ReceivedData> ReadAsync()
        {
            if (!await _dataBuffer
                .Reader
                .WaitToReadAsync()
                .ConfigureAwait(false))
            {
                throw new InvalidOperationException(ExceptionMessages.ChannelReadErrorMessage);
            }

            return await _dataBuffer
                .Reader
                .ReadAsync()
                .ConfigureAwait(false);
        }

        public async Task<IEnumerable<ReceivedData>> ReadUntilEmptyAsync()
        {
            if (!await _dataBuffer
                .Reader
                .WaitToReadAsync()
                .ConfigureAwait(false))
            {
                throw new InvalidOperationException(ExceptionMessages.ChannelReadErrorMessage);
            }

            var list = new List<ReceivedData>();
            await _dataBuffer.Reader.WaitToReadAsync().ConfigureAwait(false);
            while (_dataBuffer.Reader.TryRead(out var message))
            {
                if (message == null) { break; }
                list.Add(message);
            }

            return list;
        }

        public async IAsyncEnumerable<ReceivedData> StreamOutUntilEmptyAsync()
        {
            if (!await _dataBuffer
                .Reader
                .WaitToReadAsync()
                .ConfigureAwait(false))
            {
                throw new InvalidOperationException(ExceptionMessages.ChannelReadErrorMessage);
            }

            await _dataBuffer.Reader.WaitToReadAsync().ConfigureAwait(false);
            while (_dataBuffer.Reader.TryRead(out var message))
            {
                if (message == null) { break; }
                yield return message;
            }
        }

        public async IAsyncEnumerable<ReceivedData> StreamOutUntilClosedAsync()
        {
            if (!await _dataBuffer
                .Reader
                .WaitToReadAsync()
                .ConfigureAwait(false))
            {
                throw new InvalidOperationException(ExceptionMessages.ChannelReadErrorMessage);
            }

            await foreach (var receivedData in _dataBuffer.Reader.ReadAllAsync())
            {
                yield return receivedData;
            }
        }
        public async Task DataflowExecutionEngineAsync(Func<ReceivedData, Task<bool>> workBodyAsync, int maxDoP = 4, bool ensureOrdered = true, CancellationToken token = default)
        {
            await _dataFlowExecLock.WaitAsync(2000).ConfigureAwait(false);

            try
            {
                var dataflowEngine = new DataflowEngine<ReceivedData, bool>(workBodyAsync, maxDoP, ensureOrdered);

                while (await _dataBuffer.Reader.WaitToReadAsync(token).ConfigureAwait(false))
                {
                    while (_dataBuffer.Reader.TryRead(out var receivedData))
                    {
                        if (receivedData != null)
                        {
                            _logger.LogDebug(
                                LogMessages.Consumers.ConsumerDataflowQueueing,
                                ConsumerOptions.ConsumerName,
                                receivedData.DeliveryTag);

                            await dataflowEngine
                                .EnqueueWorkAsync(receivedData)
                                .ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning(
                    LogMessages.Consumers.ConsumerDataflowActionCancelled,
                    ConsumerOptions.ConsumerName);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    LogMessages.Consumers.ConsumerDataflowError,
                    ConsumerOptions.ConsumerName,
                    ex.Message);
            }
            finally { _dataFlowExecLock.Release(); }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _dataFlowExecLock.Dispose();
                    _conLock.Dispose();
                }

                _dataBuffer = null;
                _chanHost = null;
                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
