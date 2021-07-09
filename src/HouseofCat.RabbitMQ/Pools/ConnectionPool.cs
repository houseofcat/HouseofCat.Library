using HouseofCat.Logger;
using HouseofCat.Utilities.Errors;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace HouseofCat.RabbitMQ.Pools
{
    public interface IConnectionPool
    {
        RabbitOptions Options { get; }

        IConnection CreateConnection(string connectionName);
        ValueTask<IConnectionHost> GetConnectionAsync();
        ValueTask ReturnConnectionAsync(IConnectionHost connHost);

        Task ShutdownAsync();
    }

    public class ConnectionPool : IConnectionPool, IDisposable
    {
        private readonly ILogger<ConnectionPool> _logger;
        private readonly SemaphoreSlim _poolLock = new SemaphoreSlim(1, 1);

        private AsyncLazy<Channel<IConnectionHost>> _lazyConnections;
        private ConnectionFactory _connectionFactory;
        private bool _disposedValue;
        private ulong _currentConnectionId;

        public RabbitOptions Options { get; }

        public ConnectionPool(RabbitOptions options)
        {
            Guard.AgainstNull(options, nameof(options));
            Options = options;

            _logger = LogHelper.GetLogger<ConnectionPool>();

            _lazyConnections = new AsyncLazy<Channel<IConnectionHost>>(
                CreateConnectionsAsync, AsyncLazyFlags.ExecuteOnCallingThread | AsyncLazyFlags.RetryOnFailure);
            _connectionFactory = CreateConnectionFactory();
        }
        
        private ConnectionFactory CreateConnectionFactory()
        {
            var cf = new ConnectionFactory
            {
                Uri = Options.FactoryOptions.Uri,
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = Options.FactoryOptions.TopologyRecovery,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(Options.FactoryOptions.NetRecoveryTimeout),
                ContinuationTimeout = TimeSpan.FromSeconds(Options.FactoryOptions.ContinuationTimeout),
                RequestedHeartbeat = TimeSpan.FromSeconds(Options.FactoryOptions.HeartbeatInterval),
                RequestedChannelMax = Options.FactoryOptions.MaxChannelsPerConnection,
                DispatchConsumersAsync = Options.FactoryOptions.EnableDispatchConsumersAsync,
            };

            if (Options.FactoryOptions.SslOptions.EnableSsl)
            {
                cf.Ssl = new SslOption
                {
                    Enabled = Options.FactoryOptions.SslOptions.EnableSsl,
                    AcceptablePolicyErrors = Options.FactoryOptions.SslOptions.AcceptedPolicyErrors,
                    ServerName = Options.FactoryOptions.SslOptions.CertServerName,
                    CertPath = Options.FactoryOptions.SslOptions.LocalCertPath,
                    CertPassphrase = Options.FactoryOptions.SslOptions.LocalCertPassword,
                    Version = Options.FactoryOptions.SslOptions.ProtocolVersions
                };
            }

            return cf;
        }

        public IConnection CreateConnection(string connectionName) => _connectionFactory.CreateConnection(connectionName);

        // Allows overriding the mechanism for creating ConnectionHosts while a base one was implemented.
        protected virtual IConnectionHost CreateConnectionHost(ulong connectionId, IConnection connection) =>
            new ConnectionHost(connectionId, connection);

        private async Task<Channel<IConnectionHost>> CreateConnectionsAsync()
        {
            _logger.LogTrace(LogMessages.ConnectionPools.CreateConnections);

            var connections = Channel.CreateBounded<IConnectionHost>(Options.PoolOptions.MaxConnections);
            
            for (var i = 0; i < Options.PoolOptions.MaxConnections; i++)
            {
                var serviceName = string.IsNullOrEmpty(Options.PoolOptions.ServiceName) ? $"HoC.RabbitMQ:{i}" : $"{Options.PoolOptions.ServiceName}:{i}";
                try
                {
                    await connections
                        .Writer
                        .WriteAsync(CreateConnectionHost(_currentConnectionId++, CreateConnection(serviceName)));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, LogMessages.ConnectionPools.CreateConnectionException, serviceName);
                    throw; // Non Optional Throw
                }
            }

            _logger.LogTrace(LogMessages.ConnectionPools.CreateConnectionsComplete);
            
            return connections;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<IConnectionHost> GetConnectionAsync()
        {
            var connections = await _lazyConnections.ConfigureAwait(false);
            if (!await connections
                .Reader
                .WaitToReadAsync()
                .ConfigureAwait(false))
            {
                throw new InvalidOperationException(ExceptionMessages.GetConnectionErrorMessage);
            }

            while (true)
            {
                var connHost = await connections
                    .Reader
                    .ReadAsync().ConfigureAwait(false);

                // Connection Health Check
                var healthy = await connHost.HealthyAsync().ConfigureAwait(false);
                if (!healthy)
                {
                    await ReturnConnectionAsync(connHost).ConfigureAwait(false);
                    await Task.Delay(Options.PoolOptions.SleepOnErrorInterval).ConfigureAwait(false);
                    continue;
                }

                return connHost;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask ReturnConnectionAsync(IConnectionHost connHost)
        {
            var connections = await _lazyConnections.ConfigureAwait(false);
            if (!await connections
                    .Writer
                    .WaitToWriteAsync()
                    .ConfigureAwait(false))
            {
                throw new InvalidOperationException(ExceptionMessages.GetConnectionErrorMessage);
            }

            await connections
                .Writer
                .WriteAsync(connHost);
        }

        public async Task ShutdownAsync()
        {
            _logger.LogTrace(LogMessages.ConnectionPools.Shutdown);

            await _poolLock
                .WaitAsync()
                .ConfigureAwait(false);

            var connections = await _lazyConnections.ConfigureAwait(false);
            connections.Writer.Complete();

            await connections.Reader.WaitToReadAsync().ConfigureAwait(false);
            while (connections.Reader.TryRead(out IConnectionHost connHost))
            {
                try
                { connHost.Close(); }
                catch { }
            }

            _poolLock.Release();

            _logger.LogTrace(LogMessages.ConnectionPools.ShutdownComplete);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _poolLock.Dispose();
                }

                _connectionFactory = null;
                _lazyConnections = null;
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
