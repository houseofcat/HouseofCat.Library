using HouseofCat.Compression;
using HouseofCat.Encryption;
using HouseofCat.Logger;
using HouseofCat.RabbitMQ.Pipelines;
using HouseofCat.RabbitMQ.Pools;
using HouseofCat.RabbitMQ.WorkState;
using HouseofCat.Serialization;
using HouseofCat.Utilities.Errors;
using HouseofCat.Utilities.File;
using HouseofCat.Utilities.Time;
using HouseofCat.Dataflows.Pipelines;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace HouseofCat.RabbitMQ.Services
{
    public interface IRabbitService
    {
        IPublisher Publisher { get; }
        IChannelPool ChannelPool { get; }
        ITopologer Topologer { get; }
        RabbitOptions Options { get; }

        ISerializationProvider SerializationProvider { get; }
        IEncryptionProvider EncryptionProvider { get; }
        ICompressionProvider CompressionProvider { get; }

        ConcurrentDictionary<string, IConsumer<ReceivedData>> Consumers { get; }

        Task ComcryptAsync(IMessage message);
        Task<bool> CompressAsync(IMessage message);
        IConsumerPipeline<TOut> CreateConsumerPipeline<TOut>(string consumerName, int batchSize, bool? ensureOrdered, Func<int, bool?, IPipeline<ReceivedData, TOut>> pipelineBuilder) where TOut : RabbitWorkState;
        IConsumerPipeline<TOut> CreateConsumerPipeline<TOut>(string consumerName, IPipeline<ReceivedData, TOut> pipeline) where TOut : RabbitWorkState;

        IConsumerPipeline<TOut> CreateConsumerPipeline<TOut>(string consumerName, Func<int, bool?, IPipeline<ReceivedData, TOut>> pipelineBuilder) where TOut : RabbitWorkState;

        Task DecomcryptAsync(IMessage message);
        Task<bool> DecompressAsync(IMessage message);
        bool Decrypt(IMessage message);
        bool Encrypt(IMessage message);
        Task<ReadOnlyMemory<byte>?> GetAsync(string queueName);
        Task<T> GetAsync<T>(string queueName);
        IConsumer<ReceivedData> GetConsumer(string consumerName);
        IConsumer<ReceivedData> GetConsumerByPipelineName(string consumerPipelineName);

        ValueTask ShutdownAsync(bool immediately);
    }

    public class RabbitService : IRabbitService, IDisposable
    {
        private readonly SemaphoreSlim _serviceLock = new SemaphoreSlim(1, 1);
        private bool _disposedValue;

        public RabbitOptions Options { get; }
        public IChannelPool ChannelPool { get; }
        public IPublisher Publisher { get; }
        public ITopologer Topologer { get; }

        public ISerializationProvider SerializationProvider { get; }
        public IEncryptionProvider EncryptionProvider { get; }
        public ICompressionProvider CompressionProvider { get; }

        public ConcurrentDictionary<string, IConsumer<ReceivedData>> Consumers { get; private set; } = new ConcurrentDictionary<string, IConsumer<ReceivedData>>();
        private ConcurrentDictionary<string, ConsumerOptions> ConsumerPipelineNameToConsumerOptions { get; set; } = new ConcurrentDictionary<string, ConsumerOptions>();

        public RabbitService(
            string fileNamePath,
            ISerializationProvider serializationProvider,
            IEncryptionProvider encryptionProvider = null,
            ICompressionProvider compressionProvider = null,
            ILoggerFactory loggerFactory = null, Func<IPublishReceipt, ValueTask> processReceiptAsync = null)
            : this(
                  Utf8JsonFileReader
                    .ReadFileAsync<RabbitOptions>(fileNamePath)
                    .GetAwaiter()
                    .GetResult(),
                  serializationProvider,
                  encryptionProvider,
                  compressionProvider,
                  loggerFactory,
                  processReceiptAsync)
        { }

        public RabbitService(
            RabbitOptions options,
            ISerializationProvider serializationProvider,
            IEncryptionProvider encryptionProvider = null,
            ICompressionProvider compressionProvider = null,
            ILoggerFactory loggerFactory = null,
            Func<IPublishReceipt, ValueTask> processReceiptAsync = null) : this(
                new ChannelPool(options),
                serializationProvider,
                encryptionProvider,
                compressionProvider,
                loggerFactory,
                processReceiptAsync)
        { }

        public RabbitService(
            IChannelPool chanPool,
            ISerializationProvider serializationProvider,
            IEncryptionProvider encryptionProvider = null,
            ICompressionProvider compressionProvider = null,
            ILoggerFactory loggerFactory = null,
            Func<IPublishReceipt, ValueTask> processReceiptAsync = null)
        {
            Guard.AgainstNull(chanPool, nameof(chanPool));
            Guard.AgainstNull(serializationProvider, nameof(serializationProvider));
            LogHelper.LoggerFactory = loggerFactory;

            Options = chanPool.Options;
            ChannelPool = chanPool;

            SerializationProvider = serializationProvider;
            EncryptionProvider = encryptionProvider;
            CompressionProvider = compressionProvider;

            Publisher = new Publisher(ChannelPool, SerializationProvider, EncryptionProvider, CompressionProvider);
            Topologer = new Topologer(ChannelPool);

            Options.ApplyGlobalConsumerOptions();
            BuildConsumers();

            Publisher
                .StartAutoPublishAsync(processReceiptAsync)
                .GetAwaiter()
                .GetResult();

            BuildConsumerTopology()
                .GetAwaiter()
                .GetResult();
        }
        public async ValueTask ShutdownAsync(bool immediately)
        {
            await _serviceLock.WaitAsync().ConfigureAwait(false);

            try
            {
                await Publisher
                    .StopAutoPublishAsync(immediately)
                    .ConfigureAwait(false);

                await StopAllConsumers(immediately)
                    .ConfigureAwait(false);

                await ChannelPool
                    .ShutdownAsync()
                    .ConfigureAwait(false);
            }
            finally
            { _serviceLock.Release(); }
        }

        private async ValueTask StopAllConsumers(bool immediately)
        {
            foreach (var kvp in Consumers)
            {
                await kvp
                    .Value
                    .StopConsumerAsync(immediately)
                    .ConfigureAwait(false);
            }
        }

        private void BuildConsumers()
        {
            foreach (var consumerSetting in Options.ConsumerOptions)
            {
                if (!string.IsNullOrEmpty(consumerSetting.Value.ConsumerPipelineOptions.ConsumerPipelineName))
                {
                    ConsumerPipelineNameToConsumerOptions.TryAdd(consumerSetting.Value.ConsumerPipelineOptions.ConsumerPipelineName, consumerSetting.Value);
                }
                Consumers.TryAdd(consumerSetting.Value.ConsumerName, new Consumer(ChannelPool, consumerSetting.Value));
            }
        }

        private async Task BuildConsumerTopology()
        {
            foreach (var consumer in Consumers)
            {
                if (!string.IsNullOrWhiteSpace(consumer.Value.ConsumerOptions.QueueName))
                {
                    if (consumer.Value.ConsumerOptions.QueueArgs == null
                        || consumer.Value.ConsumerOptions.QueueArgs.Count == 0)
                    {
                        await Topologer
                            .CreateQueueAsync(consumer.Value.ConsumerOptions.QueueName)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await Topologer
                            .CreateQueueAsync(
                                consumer.Value.ConsumerOptions.QueueName,
                                true,
                                false,
                                false,
                                consumer.Value.ConsumerOptions.QueueArgs)
                            .ConfigureAwait(false);
                    }
                }

                if (!string.IsNullOrWhiteSpace(consumer.Value.ConsumerOptions.TargetQueueName))
                {
                    if (consumer.Value.ConsumerOptions.TargetQueueArgs == null
                        || consumer.Value.ConsumerOptions.TargetQueueArgs.Count == 0)
                    {
                        await Topologer
                            .CreateQueueAsync(consumer.Value.ConsumerOptions.TargetQueueName)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await Topologer
                            .CreateQueueAsync(
                                consumer.Value.ConsumerOptions.TargetQueueName,
                                true,
                                false,
                                false,
                                consumer.Value.ConsumerOptions.TargetQueueArgs)
                            .ConfigureAwait(false);
                    }
                }

                if (!string.IsNullOrWhiteSpace(consumer.Value.ConsumerOptions.ErrorSuffix)
                    && !string.IsNullOrWhiteSpace(consumer.Value.ConsumerOptions.ErrorQueueName))
                {
                    if (consumer.Value.ConsumerOptions.ErrorQueueArgs == null
                        || consumer.Value.ConsumerOptions.ErrorQueueArgs.Count == 0)
                    {
                        await Topologer
                            .CreateQueueAsync(consumer.Value.ConsumerOptions.ErrorQueueName)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await Topologer
                            .CreateQueueAsync(
                                consumer.Value.ConsumerOptions.ErrorQueueName,
                                true,
                                false,
                                false,
                                consumer.Value.ConsumerOptions.ErrorQueueArgs)
                            .ConfigureAwait(false);
                    }
                }

                if (!string.IsNullOrWhiteSpace(consumer.Value.ConsumerOptions.AltSuffix)
                    && !string.IsNullOrWhiteSpace(consumer.Value.ConsumerOptions.AltQueueName))
                {
                    if (consumer.Value.ConsumerOptions.AltQueueArgs == null
                        || consumer.Value.ConsumerOptions.AltQueueArgs.Count == 0)
                    {
                        await Topologer
                            .CreateQueueAsync(consumer.Value.ConsumerOptions.AltQueueName)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await Topologer
                            .CreateQueueAsync(
                                consumer.Value.ConsumerOptions.AltQueueName,
                                true,
                                false,
                                false,
                                consumer.Value.ConsumerOptions.AltQueueArgs)
                            .ConfigureAwait(false);
                    }
                }
            }
        }

        public IConsumerPipeline<TOut> CreateConsumerPipeline<TOut>(
            string consumerName,
            int batchSize,
            bool? ensureOrdered,
            Func<int, bool?, IPipeline<ReceivedData, TOut>> pipelineBuilder)
            where TOut : RabbitWorkState
        {
            var consumer = GetConsumer(consumerName);
            var pipeline = pipelineBuilder.Invoke(batchSize, ensureOrdered);

            return new ConsumerPipeline<TOut>(consumer, pipeline);
        }

        public IConsumerPipeline<TOut> CreateConsumerPipeline<TOut>(
            string consumerName,
            Func<int, bool?, IPipeline<ReceivedData, TOut>> pipelineBuilder)
            where TOut : RabbitWorkState
        {
            var consumer = GetConsumer(consumerName);
            var pipeline = pipelineBuilder.Invoke(
                consumer.ConsumerOptions.ConsumerPipelineOptions.MaxDegreesOfParallelism ?? 1,
                consumer.ConsumerOptions.ConsumerPipelineOptions.EnsureOrdered ?? true);

            return new ConsumerPipeline<TOut>(consumer, pipeline);
        }

        public IConsumerPipeline<TOut> CreateConsumerPipeline<TOut>(
            string consumerName,
            IPipeline<ReceivedData, TOut> pipeline)
            where TOut : RabbitWorkState
        {
            var consumer = GetConsumer(consumerName);

            return new ConsumerPipeline<TOut>(consumer, pipeline);
        }

        public IConsumer<ReceivedData> GetConsumer(string consumerName)
        {
            if (!Consumers.ContainsKey(consumerName)) throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, ExceptionMessages.NoConsumerOptionsMessage, consumerName));
            return Consumers[consumerName];
        }

        public IConsumer<ReceivedData> GetConsumerByPipelineName(string consumerPipelineName)
        {
            if (!ConsumerPipelineNameToConsumerOptions.ContainsKey(consumerPipelineName)) throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, ExceptionMessages.NoConsumerPipelineOptionsMessage, consumerPipelineName));
            if (!Consumers.ContainsKey(ConsumerPipelineNameToConsumerOptions[consumerPipelineName].ConsumerName)) throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, ExceptionMessages.NoConsumerOptionsMessage, ConsumerPipelineNameToConsumerOptions[consumerPipelineName].ConsumerName));
            return Consumers[ConsumerPipelineNameToConsumerOptions[consumerPipelineName].ConsumerName];
        }

        public async Task DecomcryptAsync(IMessage message)
        {
            var decrypted = Decrypt(message);

            if (decrypted)
            {
                await DecompressAsync(message).ConfigureAwait(false);
            }
        }

        public async Task ComcryptAsync(IMessage message)
        {
            await CompressAsync(message).ConfigureAwait(false);

            Encrypt(message);
        }

        // Returns Success
        public bool Encrypt(IMessage message)
        {
            var metadata = message.GetMetadata();
            if (!metadata.Encrypted)
            {
                message.Body = EncryptionProvider.Encrypt(message.Body).Array;
                metadata.Encrypted = true;
                metadata.CustomFields[Constants.HeaderForEncrypted] = true;
                metadata.CustomFields[Constants.HeaderForEncryption] = EncryptionProvider.Type;
                metadata.CustomFields[Constants.HeaderForEncryptDate] = Time.GetDateTimeNow(Time.Formats.CatRFC3339);

                return true;
            }

            return false;
        }

        // Returns Success
        public bool Decrypt(IMessage message)
        {
            var metadata = message.GetMetadata();
            if (metadata.Encrypted)
            {
                message.Body = EncryptionProvider.Decrypt(message.Body).Array;
                metadata.Encrypted = false;
                metadata.CustomFields[Constants.HeaderForEncrypted] = false;

                if (metadata.CustomFields.ContainsKey(Constants.HeaderForEncryption))
                {
                    metadata.CustomFields.Remove(Constants.HeaderForEncryption);
                }

                if (metadata.CustomFields.ContainsKey(Constants.HeaderForEncryptDate))
                {
                    metadata.CustomFields.Remove(Constants.HeaderForEncryptDate);
                }

                return true;
            }

            return false;
        }

        // Returns Success
        public async Task<bool> CompressAsync(IMessage message)
        {
            var metadata = message.GetMetadata();
            if (metadata.Encrypted)
            { return false; } // Don't compress after encryption.

            if (!metadata.Compressed)
            {
                message.Body = (await CompressionProvider.CompressAsync(message.Body).ConfigureAwait(false)).ToArray();
                metadata.Compressed = true;
                metadata.CustomFields[Constants.HeaderForCompressed] = true;
                metadata.CustomFields[Constants.HeaderForCompression] = CompressionProvider.Type;

                return true;
            }

            return true;
        }

        // Returns Success
        public async Task<bool> DecompressAsync(IMessage message)
        {
            var metadata = message.GetMetadata();
            if (metadata.Encrypted)
            { return false; } // Don't decompress before decryption.

            if (metadata.Compressed)
            {
                try
                {
                    message.Body = (await CompressionProvider.DecompressAsync(message.Body).ConfigureAwait(false)).ToArray();
                    metadata.Compressed = false;
                    metadata.CustomFields[Constants.HeaderForCompressed] = false;

                    if (metadata.CustomFields.ContainsKey(Constants.HeaderForCompression))
                    {
                        metadata.CustomFields.Remove(Constants.HeaderForCompression);
                    }
                }
                catch { return false; }
            }

            return true;
        }

        public async Task<ReadOnlyMemory<byte>?> GetAsync(string queueName)
        {
            IChannelHost chanHost;

            try
            {
                chanHost = await ChannelPool
                    .GetChannelAsync()
                    .ConfigureAwait(false);
            }
            catch { return default; }

            BasicGetResult result = null;
            var error = false;
            try
            {
                var channel = await chanHost.GetChannelAsync().ConfigureAwait(false);
                result = channel.BasicGet(queueName, true);
            }
            catch { error = true; }
            finally
            {
                await ChannelPool
                    .ReturnChannelAsync(chanHost, error)
                    .ConfigureAwait(false);
            }

            return result?.Body;
        }

        /// <summary>
        /// Simple retrieve message (byte[]) from queue and convert to type T. Default (assumed null) if nothing was available (or on transmission error).
        /// <para>AutoAcks message.</para>
        /// </summary>
        /// <param name="queueName"></param>
        public async Task<T> GetAsync<T>(string queueName)
        {
            IChannelHost chanHost;

            try
            {
                chanHost = await ChannelPool
                    .GetChannelAsync()
                    .ConfigureAwait(false);
            }
            catch { return default; }

            BasicGetResult result = null;
            var error = false;
            try
            {
                var channel = await chanHost.GetChannelAsync().ConfigureAwait(false);
                result = channel.BasicGet(queueName, true);
            }
            catch { error = true; }
            finally
            {
                await ChannelPool
                    .ReturnChannelAsync(chanHost, error)
                    .ConfigureAwait(false);
            }

            return result != null ? SerializationProvider.Deserialize<T>(result.Body) : default;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _serviceLock.Dispose();
                }

                Consumers = null;
                ConsumerPipelineNameToConsumerOptions = null;
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
