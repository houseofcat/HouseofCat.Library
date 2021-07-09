using HouseofCat.Compression;
using HouseofCat.Encryption;
using HouseofCat.Logger;
using HouseofCat.RabbitMQ.Pools;
using HouseofCat.Serialization;
using HouseofCat.Utilities.Errors;
using HouseofCat.Utilities.Time;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace HouseofCat.RabbitMQ
{
    public interface IPublisher
    {
        bool AutoPublisherStarted { get; }
        RabbitOptions Options { get; }

        ChannelReader<IPublishReceipt> GetReceiptBufferReader();
        Task PublishAsync(IMessage message, bool createReceipt, bool withHeaders = true);
        Task PublishWithConfirmationAsync(IMessage message, bool createReceipt, bool withHeaders = true);

        Task<bool> PublishAsync(
            string exchangeName,
            string routingKey,
            ReadOnlyMemory<byte> payload,
            bool mandatory = false,
            IBasicProperties messageProperties = null,
            string messageId = null);

        Task<bool> PublishAsync(
            string exchangeName,
            string routingKey,
            ReadOnlyMemory<byte> payload,
            IDictionary<string, object> headers = null,
            string messageId = null,
            byte? priority = 0,
            bool mandatory = false);

        Task<bool> PublishBatchAsync(
            string exchangeName,
            string routingKey,
            IList<ReadOnlyMemory<byte>> payloads,
            bool mandatory = false,
            IBasicProperties messageProperties = null);

        Task<bool> PublishBatchAsync(
            string exchangeName,
            string routingKey,
            IList<ReadOnlyMemory<byte>> payloads,
            IDictionary<string, object> headers = null,
            byte? priority = 0,
            bool mandatory = false);

        Task PublishManyAsBatchAsync(IList<IMessage> messages, bool createReceipt, bool withHeaders = true);
        Task PublishManyAsync(IList<IMessage> messages, bool createReceipt, bool withHeaders = true);
        ValueTask QueueMessageAsync(IMessage message);
        Task StartAutoPublishAsync(Func<IPublishReceipt, ValueTask> processReceiptAsync = null);
        Task StopAutoPublishAsync(bool immediately = false);
    }

    public class Publisher : IPublisher, IDisposable
    {
        public RabbitOptions Options { get; }

        public bool AutoPublisherStarted { get; private set; }

        private readonly ILogger<Publisher> _logger;
        private readonly IChannelPool _channelPool;
        private readonly SemaphoreSlim _pubLock = new SemaphoreSlim(1, 1);

        private readonly ISerializationProvider _serializationProvider;
        private readonly IEncryptionProvider _encryptionProvider;
        private readonly ICompressionProvider _compressionProvider;
        private readonly bool _withHeaders;
        private readonly bool _compress;
        private readonly bool _encrypt;
        private readonly bool _createPublishReceipts;
        private readonly TimeSpan _waitForConfirmation;

        private Channel<IMessage> _messageQueue;
        private Channel<IPublishReceipt> _receiptBuffer;

        private Task _publishingTask;
        private Task _processReceiptsAsync;
        private bool _disposedValue;

        public Publisher(
            RabbitOptions options,
            ISerializationProvider serializationProvider,
            IEncryptionProvider encryptionProvider = null,
            ICompressionProvider compressionProvider = null)
            : this(
                  new ChannelPool(options),
                  serializationProvider,
                  encryptionProvider,
                  compressionProvider)
        { }

        public Publisher(
            IChannelPool channelPool,
            ISerializationProvider serializationProvider,
            IEncryptionProvider encryptionProvider = null,
            ICompressionProvider compressionProvider = null)
        {
            Guard.AgainstNull(channelPool, nameof(channelPool));
            Guard.AgainstNull(serializationProvider, nameof(serializationProvider));

            Options = channelPool.Options;
            _logger = LogHelper.GetLogger<Publisher>();
            _serializationProvider = serializationProvider;

            if (Options.PublisherOptions.Encrypt && encryptionProvider == null)
            {
                _encrypt = false;
                _logger.LogWarning("Encryption disabled, encryptionProvider provided was null.");
            }
            else if (Options.PublisherOptions.Encrypt)
            {
                _encrypt = true;
                _encryptionProvider = encryptionProvider;
            }

            if (Options.PublisherOptions.Compress && compressionProvider == null)
            {
                _compress = false;
                _logger.LogWarning("Compression disabled, compressionProvider provided was null.");
            }
            else if (Options.PublisherOptions.Compress)
            {
                _compress = true;
                _compressionProvider = compressionProvider;
            }

            _channelPool = channelPool;
            _receiptBuffer = Channel.CreateBounded<IPublishReceipt>(
                new BoundedChannelOptions(1024)
                {
                    SingleWriter = false,
                    SingleReader = false,
                    FullMode = BoundedChannelFullMode.DropOldest, // never block
                });

            _withHeaders = Options.PublisherOptions.WithHeaders;
            _createPublishReceipts = Options.PublisherOptions.CreatePublishReceipts;
            _waitForConfirmation = TimeSpan.FromMilliseconds(Options.PublisherOptions.WaitForConfirmationTimeoutInMilliseconds);
        }

        public async Task StartAutoPublishAsync(Func<IPublishReceipt, ValueTask> processReceiptAsync = null)
        {
            await _pubLock.WaitAsync().ConfigureAwait(false);

            try
            {
                _messageQueue = Channel.CreateBounded<IMessage>(
                    new BoundedChannelOptions(Options.PublisherOptions.LetterQueueBufferSize)
                    {
                        FullMode = Options.PublisherOptions.BehaviorWhenFull
                    });

                _publishingTask = ProcessMessagesAsync(_messageQueue.Reader);

                if (processReceiptAsync == null)
                { processReceiptAsync = ProcessReceiptAsync; }

                if (_processReceiptsAsync == null)
                {
                    _processReceiptsAsync = ProcessReceiptsAsync(processReceiptAsync);
                }

                AutoPublisherStarted = true;
            }
            finally { _pubLock.Release(); }
        }

        public async Task StopAutoPublishAsync(bool immediately = false)
        {
            await _pubLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (AutoPublisherStarted)
                {
                    _messageQueue.Writer.Complete();

                    if (!immediately)
                    {
                        await _messageQueue
                            .Reader
                            .Completion
                            .ConfigureAwait(false);

                        // Wait for Publishing To Finish.
                        while (!_publishingTask.IsCompleted)
                        {
                            await Task.Delay(10).ConfigureAwait(false);
                        }
                    }

                    AutoPublisherStarted = false;
                }
            }
            finally
            { _pubLock.Release(); }
        }

        public ChannelReader<IPublishReceipt> GetReceiptBufferReader()
        {
            return _receiptBuffer.Reader;
        }

        #region AutoPublisher

        public async ValueTask QueueMessageAsync(IMessage message)
        {
            if (!AutoPublisherStarted) throw new InvalidOperationException(ExceptionMessages.AutoPublisherNotStartedError);
            Guard.AgainstNull(message, nameof(message));

            if (!await _messageQueue
                 .Writer
                 .WaitToWriteAsync()
                 .ConfigureAwait(false))
            {
                throw new InvalidOperationException(ExceptionMessages.QueueChannelError);
            }

            var metadata = message.GetMetadata();
            _logger.LogDebug(LogMessages.AutoPublishers.MessageQueued, message.MessageId, metadata?.Id);

            await _messageQueue
                .Writer
                .WriteAsync(message)
                .ConfigureAwait(false);
        }

        private async Task ProcessMessagesAsync(ChannelReader<IMessage> channelReader)
        {
            await Task.Yield();
            while (await channelReader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (channelReader.TryRead(out var message))
                {
                    if (message == null)
                    { continue; }

                    var metadata = message.GetMetadata();

                    if (_compress)
                    {
                        message.Body = _compressionProvider.Compress(message.Body).ToArray();
                        metadata.Compressed = _compress;
                        metadata.CustomFields[Constants.HeaderForCompressed] = _compress;
                        metadata.CustomFields[Constants.HeaderForCompression] = _compressionProvider.Type;
                    }

                    if (_encrypt)
                    {
                        message.Body = _encryptionProvider.Encrypt(message.Body).ToArray();
                        metadata.Encrypted = _encrypt;
                        metadata.CustomFields[Constants.HeaderForEncrypted] = _encrypt;
                        metadata.CustomFields[Constants.HeaderForEncryption] = _encryptionProvider.Type;
                        metadata.CustomFields[Constants.HeaderForEncryptDate] = Time.GetDateTimeNow(Time.Formats.CatRFC3339);
                    }

                    _logger.LogDebug(LogMessages.AutoPublishers.MessagePublished, message.MessageId, metadata?.Id);

                    await PublishAsync(message, _createPublishReceipts, _withHeaders)
                        .ConfigureAwait(false);
                }
            }
        }

        private async Task ProcessReceiptsAsync(Func<IPublishReceipt, ValueTask> processReceiptAsync)
        {
            await Task.Yield();
            await foreach (var receipt in _receiptBuffer.Reader.ReadAllAsync())
            {
                await processReceiptAsync(receipt).ConfigureAwait(false);
            }
        }

        // Super simple version to bake in requeueing of all failed to publish messages.
        private async ValueTask ProcessReceiptAsync(IPublishReceipt receipt)
        {
            var originalMessage = receipt.GetOriginalMessage();
            if (receipt.IsError && originalMessage != null)
            {
                if (AutoPublisherStarted)
                {
                    _logger.LogWarning($"Failed publish for message ({originalMessage.MessageId}). Retrying with AutoPublishing...");

                    try
                    { await QueueMessageAsync(receipt.GetOriginalMessage()); }
                    catch (Exception ex) /* No-op */
                    { _logger.LogDebug("Error ({0}) occurred on retry, most likely because retry during shutdown.", ex.Message); }
                }
                else
                {
                    _logger.LogError($"Failed publish for message ({originalMessage.MessageId}). Unable to retry as the original message was not received.");
                }
            }
        }

        #endregion

        #region Publishing

        // A basic implementation of publish but using the ChannelPool. If message properties is null, one is created and all messages are set to persistent.
        public async Task<bool> PublishAsync(
            string exchangeName,
            string routingKey,
            ReadOnlyMemory<byte> payload,
            bool mandatory = false,
            IBasicProperties messageProperties = null,
            string messageId = null)
        {
            Guard.AgainstBothNullOrEmpty(exchangeName, nameof(exchangeName), routingKey, nameof(routingKey));

            var error = false;
            var channelHost = await _channelPool.GetChannelAsync().ConfigureAwait(false);
            if (messageProperties == null)
            {
                var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
                messageProperties = channel.CreateBasicProperties();
                messageProperties.DeliveryMode = 2;
                messageProperties.MessageId = messageId ?? Guid.NewGuid().ToString();

                if (!messageProperties.IsHeadersPresent())
                {
                    messageProperties.Headers = new Dictionary<string, object>();
                }
            }

            // Non-optional Header.
            messageProperties.Headers[Constants.HeaderForObjectType] = Constants.HeaderValueForMessage;

            try
            {
                var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
                channel
                    .BasicPublish(
                        exchange: exchangeName ?? string.Empty,
                        routingKey: routingKey,
                        mandatory: mandatory,
                        basicProperties: messageProperties,
                        body: payload);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(LogMessages.Publishers.PublishFailed, $"{exchangeName}->{routingKey}", ex.Message);
                error = true;
            }
            finally
            {
                await _channelPool
                    .ReturnChannelAsync(channelHost, error);
            }

            return error;
        }

        // A basic implementation of publish but using the ChannelPool. If headers are provided and start with "x-", they get included in the message properties.
        public async Task<bool> PublishAsync(
            string exchangeName,
            string routingKey,
            ReadOnlyMemory<byte> payload,
            IDictionary<string, object> headers = null,
            string messageId = null,
            byte? priority = 0,
            bool mandatory = false)
        {
            Guard.AgainstBothNullOrEmpty(exchangeName, nameof(exchangeName), routingKey, nameof(routingKey));

            var error = false;
            var channelHost = await _channelPool.GetChannelAsync().ConfigureAwait(false);

            try
            {
                var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
                var buildProperties = await BuildPropertiesAsync(headers, channelHost, messageId, priority).ConfigureAwait(false);
                channel
                    .BasicPublish(
                        exchange: exchangeName ?? string.Empty,
                        routingKey: routingKey,
                        mandatory: mandatory,
                        basicProperties: buildProperties,
                        body: payload);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    LogMessages.Publishers.PublishFailed,
                    $"{exchangeName}->{routingKey}",
                    ex.Message);

                error = true;
            }
            finally
            {
                await _channelPool
                    .ReturnChannelAsync(channelHost, error);
            }

            return error;
        }

        // A basic implementation of publishing batches but using the ChannelPool. If message properties is null, one is created and all messages are set to persistent.
        public async Task<bool> PublishBatchAsync(
            string exchangeName,
            string routingKey,
            IList<ReadOnlyMemory<byte>> payloads,
            bool mandatory = false,
            IBasicProperties messageProperties = null)
        {
            Guard.AgainstBothNullOrEmpty(exchangeName, nameof(exchangeName), routingKey, nameof(routingKey));
            Guard.AgainstNullOrEmpty(payloads, nameof(payloads));

            var error = false;
            var channelHost = await _channelPool.GetChannelAsync().ConfigureAwait(false);
            if (messageProperties == null)
            {
                var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
                messageProperties = channel.CreateBasicProperties();
                messageProperties.DeliveryMode = 2;
                messageProperties.MessageId = Guid.NewGuid().ToString();

                if (!messageProperties.IsHeadersPresent())
                {
                    messageProperties.Headers = new Dictionary<string, object>();
                }
            }

            // Non-optional Header.
            messageProperties.Headers[Constants.HeaderForObjectType] = Constants.HeaderValueForMessage;

            try
            {
                var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
                var batch = channel.CreateBasicPublishBatch();

                for (var i = 0; i < payloads.Count; i++)
                {
                    batch.Add(exchangeName, routingKey, mandatory, messageProperties, payloads[i]);
                }

                batch.Publish();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    LogMessages.Publishers.PublishFailed,
                    $"{exchangeName}->{routingKey}",
                    ex.Message);

                error = true;
            }
            finally
            {
                await _channelPool
                    .ReturnChannelAsync(channelHost, error);
            }

            return error;
        }

        // A basic implementation of publishing batches but using the ChannelPool. If message properties is null, one is created and all messages are set to persistent.
        public async Task<bool> PublishBatchAsync(
            string exchangeName,
            string routingKey,
            IList<ReadOnlyMemory<byte>> payloads,
            IDictionary<string, object> headers = null,
            byte? priority = 0,
            bool mandatory = false)
        {
            Guard.AgainstBothNullOrEmpty(exchangeName, nameof(exchangeName), routingKey, nameof(routingKey));
            Guard.AgainstNullOrEmpty(payloads, nameof(payloads));

            var error = false;
            var channelHost = await _channelPool.GetChannelAsync().ConfigureAwait(false);

            try
            {
                var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
                var batch = channel.CreateBasicPublishBatch();

                for (var i = 0; i < payloads.Count; i++)
                {
                    var properties = await BuildPropertiesAsync(headers, channelHost, null, priority).ConfigureAwait(false);
                    batch.Add(exchangeName, routingKey, mandatory, properties, payloads[i]);
                }

                batch.Publish();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    LogMessages.Publishers.PublishFailed,
                    $"{exchangeName}->{routingKey}",
                    ex.Message);

                error = true;
            }
            finally
            {
                await _channelPool
                    .ReturnChannelAsync(channelHost, error);
            }

            return error;
        }

        /// <summary>
        /// Acquires a channel from the channel pool, then publishes message based on the letter/envelope parameters.
        /// <para>Only throws exception when failing to acquire channel or when creating a receipt after the ReceiptBuffer is closed.</para>
        /// </summary>
        /// <param name="message"></param>
        /// <param name="createReceipt"></param>
        /// <param name="withOptionalHeaders"></param>
        public async Task PublishAsync(IMessage message, bool createReceipt, bool withOptionalHeaders = true)
        {
            var error = false;
            var chanHost = await _channelPool
                .GetChannelAsync()
                .ConfigureAwait(false);

            try
            {
                var channel = await chanHost.GetChannelAsync().ConfigureAwait(false);
                channel
                    .BasicPublish(
                        message.Envelope.Exchange,
                        message.Envelope.RoutingKey,
                        message.Envelope.RoutingOptions?.Mandatory ?? false,
                        await message.BuildPropertiesAsync(chanHost, withOptionalHeaders).ConfigureAwait(false),
                        message.GetBodyToPublish(_serializationProvider));
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    LogMessages.Publishers.PublishMessageFailed,
                    $"{message.Envelope.Exchange}->{message.Envelope.RoutingKey}",
                    message.MessageId,
                    ex.Message);

                error = true;
            }
            finally
            {
                if (createReceipt)
                {
                    await CreateReceiptAsync(message, error)
                        .ConfigureAwait(false);
                }

                await _channelPool
                    .ReturnChannelAsync(chanHost, error);
            }
        }

        /// <summary>
        /// Acquires an ackable channel from the channel pool, then publishes message based on the letter/envelope parameters and waits for confirmation.
        /// <para>Only throws exception when failing to acquire channel or when creating a receipt after the ReceiptBuffer is closed.</para>
        /// <para>Not fully ready for production yet.</para>
        /// </summary>
        /// <param name="message"></param>
        /// <param name="createReceipt"></param>
        /// <param name="withOptionalHeaders"></param>
        public async Task PublishWithConfirmationAsync(IMessage message, bool createReceipt, bool withOptionalHeaders = true)
        {
            var error = false;
            var chanHost = await _channelPool
                .GetAckChannelAsync()
                .ConfigureAwait(false);

            try
            {
                var channel = await chanHost.GetChannelAsync().ConfigureAwait(false);
                channel.WaitForConfirmsOrDie(_waitForConfirmation);

                channel
                    .BasicPublish(
                        message.Envelope.Exchange,
                        message.Envelope.RoutingKey,
                        message.Envelope.RoutingOptions?.Mandatory ?? false,
                        await message.BuildPropertiesAsync(chanHost, withOptionalHeaders).ConfigureAwait(false),
                        message.GetBodyToPublish(_serializationProvider));

                channel.WaitForConfirmsOrDie(_waitForConfirmation);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    LogMessages.Publishers.PublishMessageFailed,
                    $"{message.Envelope.Exchange}->{message.Envelope.RoutingKey}",
                    message.MessageId,
                    ex.Message);

                error = true;
            }
            finally
            {
                if (createReceipt)
                {
                    await CreateReceiptAsync(message, error)
                        .ConfigureAwait(false);
                }

                await _channelPool
                    .ReturnChannelAsync(chanHost, error);
            }
        }

        /// <summary>
        /// Use this method to sequentially publish all messages in a list in the order received.
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="createReceipt"></param>
        /// <param name="withOptionalHeaders"></param>
        public async Task PublishManyAsync(IList<IMessage> messages, bool createReceipt, bool withOptionalHeaders = true)
        {
            var error = false;
            var chanHost = await _channelPool
                .GetChannelAsync()
                .ConfigureAwait(false);

            for (var i = 0; i < messages.Count; i++)
            {
                try
                {
                    var channel = await chanHost.GetChannelAsync().ConfigureAwait(false);
                    channel.BasicPublish(
                        messages[i].Envelope.Exchange,
                        messages[i].Envelope.RoutingKey,
                        messages[i].Envelope.RoutingOptions.Mandatory,
                        await messages[i].BuildPropertiesAsync(chanHost, withOptionalHeaders).ConfigureAwait(false),
                        messages[i].GetBodyToPublish(_serializationProvider));
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(
                        LogMessages.Publishers.PublishMessageFailed,
                        $"{messages[i].Envelope.Exchange}->{messages[i].Envelope.RoutingKey}",
                        messages[i].MessageId,
                        ex.Message);

                    error = true;
                }

                if (createReceipt)
                { await CreateReceiptAsync(messages[i], error).ConfigureAwait(false); }

                if (error) { break; }
            }

            await _channelPool.ReturnChannelAsync(chanHost, error).ConfigureAwait(false);
        }

        /// <summary>
        /// Use this method when a group of letters who have the same properties (deliverymode, messagetype, priority).
        /// <para>Receipt with no error indicates that we successfully handed off to internal library, not necessarily published.</para>
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="createReceipt"></param>
        /// <param name="withOptionalHeaders"></param>
        public async Task PublishManyAsBatchAsync(IList<IMessage> messages, bool createReceipt, bool withOptionalHeaders = true)
        {
            var error = false;
            var chanHost = await _channelPool
                .GetChannelAsync()
                .ConfigureAwait(false);

            try
            {
                if (messages.Count > 0)
                {
                    var channel = await chanHost.GetChannelAsync().ConfigureAwait(false);
                    var publishBatch = channel.CreateBasicPublishBatch();
                    for (var i = 0; i < messages.Count; i++)
                    {
                        publishBatch.Add(
                            messages[i].Envelope.Exchange,
                            messages[i].Envelope.RoutingKey,
                            messages[i].Envelope.RoutingOptions.Mandatory,
                            await messages[i].BuildPropertiesAsync(chanHost, withOptionalHeaders).ConfigureAwait(false),
                            messages[i].GetBodyToPublish(_serializationProvider).AsMemory());

                        if (createReceipt)
                        {
                            await CreateReceiptAsync(messages[i], error).ConfigureAwait(false);
                        }
                    }

                    publishBatch.Publish();
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    LogMessages.Publishers.PublishBatchFailed,
                    ex.Message);

                error = true;
            }
            finally
            { await _channelPool.ReturnChannelAsync(chanHost, error).ConfigureAwait(false); }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask CreateReceiptAsync(IMessage message, bool error)
        {
            if (!await _receiptBuffer
                .Writer
                .WaitToWriteAsync()
                .ConfigureAwait(false))
            {
                throw new InvalidOperationException(ExceptionMessages.ChannelReadErrorMessage);
            }

            await _receiptBuffer
                .Writer
                .WriteAsync(message.GetPublishReceipt(error))
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Intended to bring feature parity and include properties when publishing byte[], which you get for free when publishing with IMessage objects.
        /// </summary>
        /// <param name="headers"></param>
        /// <param name="channelHost"></param>
        /// <param name="messageId"></param>
        /// <param name="priority"></param>
        /// <param name="deliveryMode"></param>
        /// <returns></returns>
        private static async Task<IBasicProperties> BuildPropertiesAsync(
            IDictionary<string, object> headers,
            IChannelHost channelHost,
            string messageId = null,
            byte? priority = 0,
            byte? deliveryMode = 2)
        {
            var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
            var props = channel.CreateBasicProperties();
            props.DeliveryMode = deliveryMode ?? 2; // Default Persisted
            props.Priority = priority ?? 0; // Default Priority
            props.MessageId = messageId ?? Guid.NewGuid().ToString();

            if (!props.IsHeadersPresent())
            {
                props.Headers = new Dictionary<string, object>();
            }

            if (headers?.Count > 0)
            {
                foreach (var kvp in headers)
                {
                    if (kvp.Key.StartsWith(Constants.HeaderPrefix, StringComparison.OrdinalIgnoreCase))
                    {
                        props.Headers[kvp.Key] = kvp.Value;
                    }
                }
            }

            // Non-optional Header.
            props.Headers[Constants.HeaderForObjectType] = Constants.HeaderValueForMessage;

            return props;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _pubLock.Dispose();
                }

                _receiptBuffer = null;
                _messageQueue = null;
                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
