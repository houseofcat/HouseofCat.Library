using HouseofCat.Compression;
using HouseofCat.Encryption;
using HouseofCat.Hashing;
using HouseofCat.RabbitMQ;
using HouseofCat.RabbitMQ.Pools;
using HouseofCat.Serialization;
using HouseofCat.Utilities.File;
using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace HouseofCat.RabbitMQ.IntegrationTests
{
    public class ComplexTests : IClassFixture<RabbitFixture>
    {
        private readonly RabbitFixture _fixture;
        public Consumer Consumer;
        public Publisher Publisher;

        public ComplexTests(RabbitFixture fixture, ITestOutputHelper output)
        {
            _fixture = fixture;
            _fixture.Output = output;
            Consumer = new Consumer(_fixture.ChannelPool, "TestAutoPublisherConsumerName");
            Publisher = new Publisher(
                _fixture.ChannelPool,
                _fixture.SerializationProvider,
                _fixture.EncryptionProvider,
                _fixture.CompressionProvider);
        }

        [Fact]
        public async Task PublishAndCountReceipts()
        {
            await _fixture.Topologer.CreateQueueAsync("TestAutoPublisherConsumerQueue").ConfigureAwait(false);
            await Publisher.StartAutoPublishAsync().ConfigureAwait(false);

            const ulong count = 1000;

            var processReceiptsTask = ProcessReceiptsAsync(Publisher, count);
            var publishLettersTask = PublishLettersAsync(Publisher, count);

            while (!publishLettersTask.IsCompleted)
            { await Task.Delay(1).ConfigureAwait(false); }

            while (!processReceiptsTask.IsCompleted)
            { await Task.Delay(1).ConfigureAwait(false); }

            Assert.True(publishLettersTask.IsCompletedSuccessfully);
            Assert.True(processReceiptsTask.IsCompletedSuccessfully);
            Assert.False(processReceiptsTask.Result);

            // Cleanup
            await _fixture
                .Topologer
                .DeleteQueueAsync("TestAutoPublisherConsumerQueue")
                .ConfigureAwait(false);
        }

        [Fact]
        public async Task PublishAndConsume()
        {
            await _fixture.Topologer.CreateQueueAsync("TestAutoPublisherConsumerQueue").ConfigureAwait(false);
            await Publisher.StartAutoPublishAsync().ConfigureAwait(false);

            const ulong count = 1000;

            var processReceiptsTask = ProcessReceiptsAsync(Publisher, count);
            var publishLettersTask = PublishLettersAsync(Publisher, count);
            var consumeMessagesTask = ConsumeMessagesAsync(Consumer, count);

            await Task.Yield(); 

            while (!publishLettersTask.IsCompleted)
            { await Task.Delay(1).ConfigureAwait(false); }

            while (!processReceiptsTask.IsCompleted)
            { await Task.Delay(1).ConfigureAwait(false); }

            while (!consumeMessagesTask.IsCompleted)
            { await Task.Delay(1).ConfigureAwait(false); }

            Assert.True(publishLettersTask.IsCompletedSuccessfully);
            Assert.True(processReceiptsTask.IsCompletedSuccessfully);
            Assert.True(consumeMessagesTask.IsCompletedSuccessfully);
            Assert.False(processReceiptsTask.Result);
            Assert.False(consumeMessagesTask.Result);

            await _fixture.Topologer.DeleteQueueAsync("TestAutoPublisherConsumerQueue").ConfigureAwait(false);
        }

        private async Task PublishLettersAsync(Publisher apub, ulong count)
        {
            var sw = Stopwatch.StartNew();
            for (ulong i = 0; i < count; i++)
            {
                var letter = MessageExtensions.CreateSimpleRandomLetter("TestAutoPublisherConsumerQueue");
                letter.MessageId = Guid.NewGuid().ToString();

                await apub
                    .QueueMessageAsync(letter)
                    .ConfigureAwait(false);
            }
            sw.Stop();

            _fixture.Output.WriteLine($"Finished queueing all letters in {sw.ElapsedMilliseconds} ms.");
        }

        private async Task<bool> ProcessReceiptsAsync(Publisher apub, ulong count)
        {
            await Task.Yield();

            var buffer = apub.GetReceiptBufferReader();
            var receiptCount = 0ul;
            var error = false;

            var sw = Stopwatch.StartNew();
            while (receiptCount < count)
            {
                var receipt = await buffer.ReadAsync();
                receiptCount++;
                if (receipt.IsError)
                { error = true; break; }
            }
            sw.Stop();

            _fixture.Output.WriteLine($"Finished getting receipts.\r\nReceiptCount: {receiptCount} in {sw.ElapsedMilliseconds} ms.\r\nErrorStatus: {error}");

            return error;
        }

        private async Task<bool> ConsumeMessagesAsync(Consumer consumer, ulong count)
        {
            var messageCount = 0ul;
            var error = false;

            await consumer
                .StartConsumerAsync()
                .ConfigureAwait(false);

            var sw = Stopwatch.StartNew();
            while (messageCount < count)
            {
                try
                {
                    var message = await consumer.ReadAsync().ConfigureAwait(false);
                    message.AckMessage();
                    messageCount++;
                }
                catch
                { error = true; break; }
            }
            sw.Stop();

            _fixture.Output.WriteLine($"Finished consuming messages.\r\nMessageCount: {messageCount} in {sw.ElapsedMilliseconds} ms.\r\nErrorStatus: {error}");
            return error;
        }
    }
}
