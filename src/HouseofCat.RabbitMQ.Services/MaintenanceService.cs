using HouseofCat.RabbitMQ.Pools;
using HouseofCat.Utilities.Errors;
using RabbitMQ.Client;
using System.Threading.Tasks;

namespace HouseofCat.RabbitMQ.Services
{
    public interface IMaintenanceService
    {
        Task<bool> PurgeQueueAsync(IChannelPool channelPool, string queueName, bool deleteQueueAfter = false);
        Task<bool> TransferAllMessagesAsync(IChannelPool originChannelPool, IChannelPool targetChannelPool, string originQueueName, string targetQueueName);
        Task<bool> TransferAllMessagesAsync(IChannelPool channelPool, string originQueueName, string targetQueueName);
        Task<bool> TransferMessageAsync(IChannelPool originChannelPool, IChannelPool targetChannelPool, string originQueueName, string targetQueueName);
        Task<bool> TransferMessageAsync(IChannelPool channelPool, string originQueueName, string targetQueueName);
    }

    public class MaintenanceService : IMaintenanceService
    {
        public async Task<bool> PurgeQueueAsync(
            IChannelPool channelPool,
            string queueName,
            bool deleteQueueAfter = false)
        {
            Guard.AgainstNull(channelPool, nameof(channelPool));
            Guard.AgainstNullOrEmpty(queueName, nameof(queueName));

            var error = false;
            var channelHost = await channelPool.GetChannelAsync().ConfigureAwait(false);

            try
            {
                var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
                channel.QueuePurge(queueName);

                if (deleteQueueAfter)
                {
                    channel.QueueDelete(queueName, false, false);
                }
            }
            catch { error = true; }
            finally
            {
                await channelPool
                    .ReturnChannelAsync(channelHost, error);
            }

            return error;
        }

        public async Task<bool> TransferMessageAsync(
            IChannelPool channelPool,
            string originQueueName,
            string targetQueueName)
        {
            Guard.AgainstNull(channelPool, nameof(channelPool));
            Guard.AgainstNullOrEmpty(originQueueName, nameof(originQueueName));
            Guard.AgainstNullOrEmpty(targetQueueName, nameof(targetQueueName));

            var error = false;
            var channelHost = await channelPool.GetChannelAsync().ConfigureAwait(false);
            var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
            var properties = channel.CreateBasicProperties();
            properties.DeliveryMode = 2;

            try
            {
                var result = channel.BasicGet(originQueueName, true);

                if (result?.Body != null)
                {
                    channel.BasicPublish(string.Empty, targetQueueName, false, properties, result.Body);
                }
            }
            catch { error = true; }
            finally
            {
                await channelPool
                    .ReturnChannelAsync(channelHost, error);
            }

            return error;
        }

        public async Task<bool> TransferMessageAsync(
            IChannelPool originChannelPool,
            IChannelPool targetChannelPool,
            string originQueueName,
            string targetQueueName)
        {
            Guard.AgainstNull(originChannelPool, nameof(originChannelPool));
            Guard.AgainstNull(targetChannelPool, nameof(targetChannelPool));
            Guard.AgainstNullOrEmpty(originQueueName, nameof(originQueueName));
            Guard.AgainstNullOrEmpty(targetQueueName, nameof(targetQueueName));

            var error = false;
            var channelHost = await originChannelPool.GetChannelAsync().ConfigureAwait(false);
            var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
            var properties = channel.CreateBasicProperties();
            properties.DeliveryMode = 2;

            BasicGetResult result = null;
            try
            {
                result = channel.BasicGet(originQueueName, true);
            }
            catch { error = true; }
            finally
            {
                await originChannelPool
                    .ReturnChannelAsync(channelHost, error);
            }

            if (!error && result?.Body != null)
            {
                try
                {
                    var targetChannelHost = await targetChannelPool.GetChannelAsync().ConfigureAwait(false);
                    var targetChannel = await targetChannelHost.GetChannelAsync().ConfigureAwait(false);
                    targetChannel.BasicPublish(string.Empty, targetQueueName, false, properties, result.Body);
                }
                catch { error = true; }
                finally
                {
                    await targetChannelPool
                        .ReturnChannelAsync(channelHost, error);
                }
            }

            return error;
        }

        public async Task<bool> TransferAllMessagesAsync(
            IChannelPool channelPool,
            string originQueueName,
            string targetQueueName)
        {
            Guard.AgainstNull(channelPool, nameof(channelPool));
            Guard.AgainstNullOrEmpty(originQueueName, nameof(originQueueName));
            Guard.AgainstNullOrEmpty(targetQueueName, nameof(targetQueueName));

            var error = false;
            var channelHost = await channelPool.GetChannelAsync().ConfigureAwait(false);
            var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
            var properties = channel.CreateBasicProperties();
            properties.DeliveryMode = 2;

            try
            {
                BasicGetResult result = null;

                while (true)
                {
                    result = channel.BasicGet(originQueueName, true);
                    if (result == null) { break; }

                    if (result?.Body != null)
                    {
                        channel.BasicPublish(string.Empty, targetQueueName, false, properties, result.Body);
                    }
                }
            }
            catch { error = true; }
            finally
            {
                await channelPool
                    .ReturnChannelAsync(channelHost, error);
            }

            return error;
        }

        public async Task<bool> TransferAllMessagesAsync(
            IChannelPool originChannelPool,
            IChannelPool targetChannelPool,
            string originQueueName,
            string targetQueueName)
        {
            Guard.AgainstNull(originChannelPool, nameof(originChannelPool));
            Guard.AgainstNull(targetChannelPool, nameof(targetChannelPool));
            Guard.AgainstNullOrEmpty(originQueueName, nameof(originQueueName));
            Guard.AgainstNullOrEmpty(targetQueueName, nameof(targetQueueName));

            var error = false;
            var channelHost = await originChannelPool.GetChannelAsync().ConfigureAwait(false);
            var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
            var properties = channel.CreateBasicProperties();
            properties.DeliveryMode = 2;

            BasicGetResult result = null;

            while (true)
            {
                try
                {
                    result = channel.BasicGet(originQueueName, true);
                    if (result == null) { break; }
                }
                catch { error = true; }
                finally
                {
                    await originChannelPool
                        .ReturnChannelAsync(channelHost, error);
                }

                if (!error && result?.Body != null)
                {
                    try
                    {
                        var targetChannelHost = await targetChannelPool.GetChannelAsync().ConfigureAwait(false);
                        var targetChannel = await targetChannelHost.GetChannelAsync().ConfigureAwait(false);
                        targetChannel.BasicPublish(string.Empty, targetQueueName, false, properties, result.Body);
                    }
                    catch { error = true; }
                    finally
                    {
                        await targetChannelPool
                            .ReturnChannelAsync(channelHost, error);
                    }
                }
            }

            return error;
        }
    }
}
