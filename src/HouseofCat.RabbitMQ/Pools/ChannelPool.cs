using HouseofCat.Logger;
using HouseofCat.Utilities.Errors;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace HouseofCat.RabbitMQ.Pools
{
    public interface IChannelPool
    {
        RabbitOptions Options { get; }
        ulong CurrentChannelId { get; }
        bool Shutdown { get; }

        /// <summary>
        /// This pulls an ackable <see cref="IChannelHost"/> out of the <see cref="IChannelPool"/> for usage.
        /// <para>If the <see cref="IChannelHost"/> was previously flagged on error, multi-attempts to recreate it before returning an open channel back to the user.
        /// If you only remove channels and never add them, you will drain your <see cref="IChannelPool"/>.</para>
        /// <para>Use <see cref="ReturnChannelAsync"/> to return Channels.</para>
        /// <para><em>Note: During an outage event, you will pause here until a viable channel can be acquired.</em></para>
        /// </summary>
        /// <returns><see cref="IChannelHost"/></returns>
        ValueTask<IChannelHost> GetAckChannelAsync();

        /// <summary>
        /// This pulls a <see cref="IChannelHost"/> out of the <see cref="IChannelPool"/> for usage.
        /// <para>If the <see cref="IChannelHost"/> was previously flagged on error, multi-attempts to recreate it before returning an open channel back to the user.
        /// If you only remove channels and never add them, you will drain your <see cref="IChannelPool"/>.</para>
        /// <para>Use <see cref="ReturnChannelAsync"/> to return the <see cref="IChannelHost"/>.</para>
        /// <para><em>Note: During an outage event, you will pause here until a viable channel can be acquired.</em></para>
        /// </summary>
        /// <returns><see cref="IChannelHost"/></returns>
        ValueTask<IChannelHost> GetChannelAsync();

        /// <summary>
        /// <para>Gives user a transient <see cref="IChannelHost"/> is simply a channel not managed by this library.</para>
        /// <para><em>Closing and disposing the <see cref="IChannelHost"/> is the responsiblity of the user.</em></para>
        /// </summary>
        /// <param name="ackable"></param>
        /// <returns><see cref="IChannelHost"/></returns>
        ValueTask<IChannelHost> GetTransientChannelAsync(bool ackable);

        ValueTask ReturnChannelAsync(IChannelHost chanHost, bool flagChannel = false);
        Task ShutdownAsync();
    }

    public class ChannelPool : IChannelPool, IDisposable
    {
        private readonly ILogger<ChannelPool> _logger;
        private readonly IConnectionPool _connectionPool;
        private readonly SemaphoreSlim _poolLock = new SemaphoreSlim(1, 1);
        private AsyncLazy<Channel<IChannelHost>> _lazyChannels;
        private AsyncLazy<Channel<IChannelHost>> _lazyAckChannels;
        private ConcurrentDictionary<ulong, bool> _flaggedChannels;
        private bool _disposedValue;

        public RabbitOptions Options { get; }

        // A 0 indicates TransientChannels.
        public ulong CurrentChannelId { get; private set; } = 1;
        public bool Shutdown { get; private set; }

        public ChannelPool(RabbitOptions options) : this(new ConnectionPool(options)) { }

        public ChannelPool(IConnectionPool connPool)
        {
            Guard.AgainstNull(connPool, nameof(connPool));
            Options = connPool.Options;

            _logger = LogHelper.GetLogger<ChannelPool>();
            _connectionPool = connPool;
            _flaggedChannels = new ConcurrentDictionary<ulong, bool>();
            _lazyChannels = new AsyncLazy<Channel<IChannelHost>>(() => CreateChannelsAsync(false), 
                AsyncLazyFlags.ExecuteOnCallingThread | AsyncLazyFlags.RetryOnFailure);
            _lazyAckChannels = new AsyncLazy<Channel<IChannelHost>>(() => CreateChannelsAsync(true), 
                AsyncLazyFlags.ExecuteOnCallingThread | AsyncLazyFlags.RetryOnFailure);
        }

        private async Task<Channel<IChannelHost>> CreateChannelsAsync(bool ackable)
        {
            var channels = Channel.CreateBounded<IChannelHost>(Options.PoolOptions.MaxChannels);

            for (var i = 0; i < Options.PoolOptions.MaxChannels; i++)
            {
                var chanHost = await CreateChannelAsync(CurrentChannelId++, ackable).ConfigureAwait(false);

                await channels
                    .Writer
                    .WriteAsync(chanHost);
            }
            
            return channels;
        }

        /// <summary>
        /// This pulls a <see cref="IChannelHost"/> out of the <see cref="IChannelPool"/> for usage.
        /// <para>If the <see cref="IChannelHost"/> was previously flagged on error, multi-attempta to recreate it before returning an open channel back to the user.
        /// If you only remove channels and never add them, you will drain your <see cref="IChannelPool"/>.</para>
        /// <para>Use <see cref="ReturnChannelAsync"/> to return the <see cref="IChannelHost"/>.</para>
        /// <para><em>Note: During an outage event, you will pause here until a viable channel can be acquired.</em></para>
        /// </summary>
        /// <returns><see cref="IChannelHost"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<IChannelHost> GetChannelAsync()
        {
            if (Shutdown) throw new InvalidOperationException(ExceptionMessages.ChannelPoolValidationMessage);

            var channels = await _lazyChannels.ConfigureAwait(false);
            
            if (!await channels
                .Reader
                .WaitToReadAsync()
                .ConfigureAwait(false))
            {
                throw new InvalidOperationException(ExceptionMessages.ChannelPoolGetChannelError);
            }

            var chanHost = await channels
                .Reader
                .ReadAsync()
                .ConfigureAwait(false);

            var healthy = await chanHost.HealthyAsync().ConfigureAwait(false);
            var flagged = _flaggedChannels.ContainsKey(chanHost.ChannelId) && _flaggedChannels[chanHost.ChannelId];
            if (flagged || !healthy)
            {
                _logger.LogWarning(LogMessages.ChannelPools.DeadChannel, chanHost.ChannelId);

                var success = false;
                while (!success)
                {
                    success = await chanHost.MakeChannelAsync().ConfigureAwait(false);
                    await Task.Delay(Options.PoolOptions.SleepOnErrorInterval).ConfigureAwait(false);
                }
            }

            return chanHost;
        }

        /// <summary>
        /// This pulls an ackable <see cref="IChannelHost"/> out of the <see cref="IChannelPool"/> for usage.
        /// <para>If the <see cref="IChannelHost"/> was previously flagged on error, multi-attempta to recreate it before returning an open channel back to the user.
        /// If you only remove channels and never add them, you will drain your <see cref="IChannelPool"/>.</para>
        /// <para>Use <see cref="ReturnChannelAsync"/> to return Channels.</para>
        /// <para><em>Note: During an outage event, you will pause here until a viable channel can be acquired.</em></para>
        /// </summary>
        /// <returns><see cref="IChannelHost"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<IChannelHost> GetAckChannelAsync()
        {
            if (Shutdown) throw new InvalidOperationException(ExceptionMessages.ChannelPoolValidationMessage);

            var ackChannels = await _lazyAckChannels.ConfigureAwait(false);
            
            if (!await ackChannels
                .Reader
                .WaitToReadAsync()
                .ConfigureAwait(false))
            {
                throw new InvalidOperationException(ExceptionMessages.ChannelPoolGetChannelError);
            }

            var chanHost = await ackChannels
                .Reader
                .ReadAsync()
                .ConfigureAwait(false);

            var healthy = await chanHost.HealthyAsync().ConfigureAwait(false);
            var flagged = _flaggedChannels.ContainsKey(chanHost.ChannelId) && _flaggedChannels[chanHost.ChannelId];
            if (flagged || !healthy)
            {
                _logger.LogWarning(LogMessages.ChannelPools.DeadChannel, chanHost.ChannelId);

                var success = false;
                while (!success)
                {
                    await Task.Delay(Options.PoolOptions.SleepOnErrorInterval).ConfigureAwait(false);
                    success = await chanHost.MakeChannelAsync().ConfigureAwait(false);
                }
            }

            return chanHost;
        }

        /// <summary>
        /// <para>Gives user a transient <see cref="IChannelHost"/> is simply a channel not managed by this library.</para>
        /// <para><em>Closing and disposing the <see cref="IChannelHost"/> is the responsiblity of the user.</em></para>
        /// </summary>
        /// <param name="ackable"></param>
        /// <returns><see cref="IChannelHost"/></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<IChannelHost> GetTransientChannelAsync(bool ackable) => await CreateChannelAsync(0, ackable).ConfigureAwait(false);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async Task<IChannelHost> CreateChannelAsync(ulong channelId, bool ackable)
        {
            IConnectionHost connHost = null;

            while (true)
            {
                _logger.LogTrace(LogMessages.ChannelPools.CreateChannel, channelId);

                // Get ConnectionHost
                try
                { connHost = await _connectionPool.GetConnectionAsync().ConfigureAwait(false); }
                catch
                {
                    _logger.LogTrace(LogMessages.ChannelPools.CreateChannelFailedConnection, channelId);
                    await ReturnConnectionWithOptionalSleep(connHost, channelId, Options.PoolOptions.SleepOnErrorInterval).ConfigureAwait(false);
                    continue;
                }

                // Create a Channel Host
                try
                {
                    var chanHost = new ChannelHost(channelId, connHost, ackable);
                    await chanHost.MakeChannelAsync().ConfigureAwait(false);
                    await ReturnConnectionWithOptionalSleep(connHost, channelId, 0).ConfigureAwait(false);
                    _flaggedChannels[chanHost.ChannelId] = false;
                    _logger.LogDebug(LogMessages.ChannelPools.CreateChannelSuccess, channelId);

                    return chanHost;
                }
                catch
                {
                    _logger.LogTrace(LogMessages.ChannelPools.CreateChannelFailedConstruction, channelId);
                    await ReturnConnectionWithOptionalSleep(connHost, channelId, Options.PoolOptions.SleepOnErrorInterval).ConfigureAwait(false);
                }
            }
        }

        private async Task ReturnConnectionWithOptionalSleep(IConnectionHost connHost, ulong channelId, int sleep)
        {
            if (connHost != null)
            { await _connectionPool.ReturnConnectionAsync(connHost); } // Return Connection (or lose them.)

            if (sleep > 0)
            {
                _logger.LogDebug(LogMessages.ChannelPools.CreateChannelSleep, channelId);

                await Task
                    .Delay(sleep)
                    .ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns the <see cref="ChannelHost"/> back to the <see cref="ChannelPool"/>.
        /// <para>All Aqmp IModel Channels close server side on error, so you have to indicate to the library when that happens.</para>
        /// <para>The library does its best to listen for a dead <see cref="ChannelHost"/>, but nothing is as reliable as the user flagging the channel for replacement.</para>
        /// <para><em>Users flag the channel for replacement (e.g. when an error occurs) on it's next use.</em></para>
        /// </summary>
        /// <param name="chanHost"></param>
        /// <param name="flagChannel"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask ReturnChannelAsync(IChannelHost chanHost, bool flagChannel = false)
        {
            if (Shutdown) throw new InvalidOperationException(ExceptionMessages.ChannelPoolValidationMessage);

            _flaggedChannels[chanHost.ChannelId] = flagChannel;

            _logger.LogDebug(LogMessages.ChannelPools.ReturningChannel, chanHost.ChannelId, flagChannel);

            if (chanHost.Ackable)
            {
                var ackChannels = await _lazyAckChannels.ConfigureAwait(false);
                await ackChannels
                    .Writer
                    .WriteAsync(chanHost)
                    .ConfigureAwait(false);
            }
            else
            {
                var channels = await _lazyChannels.ConfigureAwait(false);
                await channels
                    .Writer
                    .WriteAsync(chanHost)
                    .ConfigureAwait(false);
            }
        }

        public async Task ShutdownAsync()
        {
            _logger.LogTrace(LogMessages.ChannelPools.Shutdown);

            await _poolLock
                .WaitAsync()
                .ConfigureAwait(false);

            if (!Shutdown)
            {
                await CloseChannelsAsync()
                    .ConfigureAwait(false);

                Shutdown = true;

                await _connectionPool
                    .ShutdownAsync()
                    .ConfigureAwait(false);
            }

            _poolLock.Release();
            _logger.LogTrace(LogMessages.ChannelPools.ShutdownComplete);
        }

        private async Task CloseChannelsAsync()
        {
            var channels = await _lazyChannels.ConfigureAwait(false);
            var ackChannels = await _lazyAckChannels.ConfigureAwait(false);
            
            // Signal to Channel no more data is coming.
            channels.Writer.Complete();
            ackChannels.Writer.Complete();

            await channels.Reader.WaitToReadAsync().ConfigureAwait(false);
            while (channels.Reader.TryRead(out IChannelHost chanHost))
            {
                try
                { await chanHost.CloseAsync().ConfigureAwait(false); }
                catch { }
            }

            await ackChannels.Reader.WaitToReadAsync().ConfigureAwait(false);
            while (ackChannels.Reader.TryRead(out IChannelHost chanHost))
            {
                try
                { await chanHost.CloseAsync().ConfigureAwait(false); }
                catch { }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _lazyChannels = null;
                    _lazyAckChannels = null;
                    _flaggedChannels = null;
                    _poolLock.Dispose();
                }

                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
