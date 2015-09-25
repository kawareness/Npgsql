using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Logging;

namespace Npgsql
{
    static class PoolManager
    {
        /// <summary>
        /// Holds connector pools indexed by their connection strings.
        /// </summary>
        static internal ConcurrentDictionary<NpgsqlConnectionStringBuilder, ConnectorPool> Pools { get; private set; }

        /// <summary>
        /// Maximum number of possible connections in the pool.
        /// </summary>
        internal const int PoolSizeLimit = 1024;

        static PoolManager()
        {
            Pools = new ConcurrentDictionary<NpgsqlConnectionStringBuilder, ConnectorPool>();
        }

        internal static ConnectorPool GetOrAdd(NpgsqlConnectionStringBuilder connString)
        {
            Contract.Requires(connString != null);
            Contract.Ensures(Contract.Result<ConnectorPool>() != null);

            return Pools.GetOrAdd(connString, cs =>
            {
                if (cs.MaxPoolSize < cs.MinPoolSize)
                    throw new ArgumentException(string.Format("Connection can't have MaxPoolSize {0} under MinPoolSize {1}", cs.MaxPoolSize, cs.MinPoolSize));
                return new ConnectorPool(cs);
            });
        }

        internal static ConnectorPool Get(NpgsqlConnectionStringBuilder connString)
        {
            Contract.Requires(connString != null);
            Contract.Ensures(Contract.Result<ConnectorPool>() != null);

            return Pools[connString];
        }
    }

    class ConnectorPool
    {
        internal NpgsqlConnectionStringBuilder ConnectionString;

        /// <summary>
        /// Open connectors waiting to be requested by new connections
        /// </summary>
        internal Stack<NpgsqlConnector> Idle;

        readonly int _max, _min;
        int _busy;

        internal Queue<TaskCompletionSource<NpgsqlConnector>> Waiting;

        static readonly NpgsqlLogger Log = NpgsqlLogManager.GetCurrentClassLogger();

        internal ConnectorPool(NpgsqlConnectionStringBuilder csb)
        {
            _max = csb.MaxPoolSize;
            _min = csb.MinPoolSize;

            ConnectionString = csb;
            Idle = new Stack<NpgsqlConnector>(_max);
            Waiting = new Queue<TaskCompletionSource<NpgsqlConnector>>();
        }

        internal NpgsqlConnector Allocate(string password, ProvideClientCertificatesCallback provideClientCertificatesCallback, RemoteCertificateValidationCallback userCertificateValidationCallback, NpgsqlTimeout timeout)
        {
            NpgsqlConnector connector;
            Monitor.Enter(this);

            if (Idle.Count > 0)
            {
                connector = Idle.Pop();
                _busy++;
                Monitor.Exit(this);
                return connector;
            }

            if (_busy >= _max)
            {
                // TODO: Async cancellation
                var tcs = new TaskCompletionSource<NpgsqlConnector>();
                Waiting.Enqueue(tcs);
                Monitor.Exit(this);
                if (!tcs.Task.Wait(timeout.TimeLeft))
                {
                    // Re-lock and check in case the task was set to completed after coming out of the Wait
                    lock (this)
                    {
                        if (!tcs.Task.IsCompleted)
                        {
                            tcs.SetCanceled();
                            throw new TimeoutException(string.Format("The connection pool has been exhausted, either raise MaxPoolSize (currently {0}) or Timeout (currently {1} seconds)",
                                                       _max, ConnectionString.Timeout));
                        }
                    }
                }
                return tcs.Task.Result;
            }

            // No idle connectors are available, and we're under the pool's maximum capacity.
            _busy++;
            Monitor.Exit(this);

            try
            {
                connector = new NpgsqlConnector(ConnectionString, password, provideClientCertificatesCallback, userCertificateValidationCallback);
                connector.Open();
                return connector;
            }
            catch
            {
                lock (this)
                {
                    _busy--;
                }
                throw;
            }
        }

        internal void Release(NpgsqlConnector connector)
        {
            connector.Reset();
            lock (this)
            {
                while (Waiting.Count > 0)
                {
                    var tcs = Waiting.Dequeue();
                    if (tcs.TrySetResult(connector)) {
                        return;
                    }
                }

                Idle.Push(connector);
                _busy--;
                Contract.Assert(Idle.Count <= _max);
            }
        }

        public override string ToString()
        {
            return string.Format("[{0} busy, {1} idle, {2} waiting]", _busy, Idle.Count, Waiting.Count);
        }

        [ContractInvariantMethod]
        void ObjectInvariants()
        {
            Contract.Invariant(_busy <= _max);
        }
    }
}
