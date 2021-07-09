using HouseofCat.Logger;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace HouseofCat.RabbitMQ.Pools
{
    public interface IChannelHost
    {
        bool Ackable { get; }
        ulong ChannelId { get; }
        bool Closed { get; }
        bool FlowControlled { get; }

        Task<IModel> GetChannelAsync();
        IModel GetChannel();
        Task<bool> MakeChannelAsync();
        Task CloseAsync();
        void Close();
        Task<bool> HealthyAsync();
    }

    public class ChannelHost : IChannelHost, IDisposable
    {
        private readonly ILogger<ChannelHost> _logger;
        private AsyncLazy<IModel> _lazyChannel { get; set; }
        private IConnectionHost _connHost { get; set; }

        public ulong ChannelId { get; set; }

        public bool Ackable { get; }

        public bool Closed { get; private set; }
        public bool FlowControlled { get; private set; }

        private readonly SemaphoreSlim _hostLock = new SemaphoreSlim(1, 1);
        private bool _disposedValue;

        public ChannelHost(ulong channelId, IConnectionHost connHost, bool ackable)
        {
            _logger = LogHelper.GetLogger<ChannelHost>();

            ChannelId = channelId;
            _connHost = connHost;
            Ackable = ackable;
            
            _lazyChannel = new AsyncLazy<IModel>(
                CreateChannelAsync, AsyncLazyFlags.ExecuteOnCallingThread | AsyncLazyFlags.RetryOnFailure);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async Task<IModel> GetChannelAsync()
        {
            await _hostLock.WaitAsync().ConfigureAwait(false);

            try
            {
                return await _lazyChannel.ConfigureAwait(false);
            }
            finally
            {
                _hostLock.Release();
            }
        }

        public IModel GetChannel() => GetChannelAsync().GetAwaiter().GetResult();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Task<IModel> CreateChannelAsync()
        {
            var channel = _connHost.Connection.CreateModel();

            if (Ackable)
            {
                channel.ConfirmSelect();
            }

            channel.FlowControl += FlowControl;
            channel.ModelShutdown += ChannelClose;

            return Task.FromResult(channel);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async Task<bool> MakeChannelAsync()
        {
            await _hostLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (_lazyChannel.IsStarted)
                {
                    var channel = await _lazyChannel.ConfigureAwait(false);
                    channel.FlowControl -= FlowControl;
                    channel.ModelShutdown -= ChannelClose;
                    await CloseAsync().ConfigureAwait(false);
                }

                _lazyChannel = new AsyncLazy<IModel>(
                    CreateChannelAsync, AsyncLazyFlags.ExecuteOnCallingThread | AsyncLazyFlags.RetryOnFailure);
                await _lazyChannel.ConfigureAwait(false);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError("Making a channel failed. Error: {0}", ex.Message);
                return false;
            }
            finally
            { _hostLock.Release(); }
        }

        protected virtual void ChannelClose(object sender, ShutdownEventArgs e)
        {
            _hostLock.Wait();
            _logger.LogDebug(e.ReplyText);
            Closed = true;
            _hostLock.Release();
        }

        protected virtual void FlowControl(object sender, FlowControlEventArgs e)
        {
            _hostLock.Wait();

            if (e.Active)
            { _logger.LogWarning(LogMessages.ChannelHosts.FlowControlled); }
            else
            { _logger.LogInformation(LogMessages.ChannelHosts.FlowControlFinished); }

            FlowControlled = e.Active;

            _hostLock.Release();
        }

        public async Task<bool> HealthyAsync()
        {
            var connectionHealthy = await _connHost.HealthyAsync().ConfigureAwait(false);
            try
            {
                var channel = await _lazyChannel.ConfigureAwait(false);
                return connectionHealthy && !FlowControlled && channel.IsOpen;
            }
            catch
            {
                return false;
            }
        }

        private const int CloseCode = 200;
        private const string CloseMessage = "HouseofCat.RabbitMQ manual close channel initiated.";

        public async Task CloseAsync()
        {
            try
            {
                var channel = await _lazyChannel.ConfigureAwait(false);
                if (!Closed || !channel.IsOpen)
                {
                    channel.Close(CloseCode, CloseMessage);
                }
            }
            catch { }
        }

        public void Close() => CloseAsync().GetAwaiter().GetResult();

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _hostLock.Dispose();
                }

                _lazyChannel = null;
                _connHost = null;
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
