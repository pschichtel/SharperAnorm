using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace SharperAnorm
{
    public abstract class Runner<TRow> : IRunner<TRow>
    {
        private readonly Func<Task<DbConnection>> _connectionProvider;
        private readonly Func<DbConnection, Task> _connectionDisposer;

        protected Runner(Func<Task<DbConnection>> connectionProvider, Func<DbConnection, Task> connectionDisposer)
        {
            _connectionProvider = connectionProvider;
            _connectionDisposer = connectionDisposer;
        }

        #region NoResult

        public Task RunNoResult(Query q)
        {
            return RunNoResult(q, default);
        }

        public async Task RunNoResult(Query q, CancellationToken ct)
        {
            await WithConnection(async c => { await RunNoResult(c, q, ct); });
        }

        public Task RunNoResult(DbConnection c, Query q, CancellationToken ct)
        {
            return RunAction(c, q, async cmd => await cmd.ExecuteNonQueryAsync(ct));
        }

        #endregion

        #region Single

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p)
        {
            return RunSingle(q, p, default);
        }

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return WithConnection(c =>
            {
                return RunAction(c, q, async cmd =>
                {
                    var result = await cmd.ExecuteReaderAsync(ct);

                    return await Single(result, p, ct);
                });
            });
        }

        internal async Task<T> RunSingle<T>(DbConnection c, Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return await RunAction(c, q, async cmd =>
            {
                var result = await cmd.ExecuteReaderAsync(ct);

                return await Single(result, p, ct);
            });
        }

        protected abstract Task<T> Single<T>(DbDataReader result, RowParser<T, TRow> parser, CancellationToken ct);

        #endregion

        #region NonQuery

        public Task<int> RunNonQuery(Query q, RowParser<int, TRow> p)
        {
            return RunNonQuery(q, p, default);
        }

        public async Task<int> RunNonQuery(Query q, RowParser<int, TRow> p, CancellationToken ct)
        {
            return await WithConnection(c => RunNonQuery(c, q, p, ct));
        }

        internal Task<int> RunNonQuery<T>(DbConnection c, Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return RunAction(c, q, async cmd => await cmd.ExecuteNonQueryAsync(ct));
        }

        #endregion

        #region Run

        protected abstract IEnumerator<T> Enumerate<T>(DbDataReader result, RowParser<T, TRow> parser);

        public Task<IEnumerator<T>> Run<T>(Query q, RowParser<T, TRow> p)
        {
            return Run(q, p, default);
        }

        public async Task<IEnumerator<T>> Run<T>(Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return await WithConnection(c => Run(c, q, p, ct));
        }

        internal Task<IEnumerator<T>> Run<T>(DbConnection c, Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return RunAction(c, q, async cmd =>
            {
                var result = await cmd.ExecuteReaderAsync(ct);

                return Enumerate(result, p);
            });
        }

        private async Task<T> RunAction<T>(DbConnection c, Query q, Func<DbCommand, Task<T>> action)
        {
            await using var cmd = c.CreateCommand();
            cmd.CommandText = q.Statement;
            foreach (var (key, value) in q.Parameters)
            {
                var param = cmd.CreateParameter();
                param.ParameterName = key;
                param.Value = value;
            }

            return await action(cmd);
        }

        #endregion

        #region WithConnection

        private async Task<T> WithConnection<T>(Func<DbConnection, Task<T>> f)
        {
            var c = await _connectionProvider();
            try
            {
                return await f(c);
            }
            finally
            {
                await _connectionDisposer(c);
            }
        }

        private async Task WithConnection(Func<DbConnection, Task> f)
        {
            var c = await _connectionProvider();
            try
            {
                await f(c);
            }
            finally
            {
                await _connectionDisposer(c);
            }
        }

        #endregion

        #region Transaction

        [HandleProcessCorruptedStateExceptions]
        public Task<T> Transaction<T>(Func<TransactionRunner<TRow>, Task<T>> f)
        {
            async Task<DbTransaction> StartTransaction(DbConnection c) => await c.BeginTransactionAsync();

            return Transaction(StartTransaction, f);
        }

        [HandleProcessCorruptedStateExceptions]
        public Task<T> Transaction<T>(IsolationLevel isolationLevel, Func<TransactionRunner<TRow>, Task<T>> f)
        {
            async Task<DbTransaction> StartTransaction(DbConnection c) => await c.BeginTransactionAsync(isolationLevel);

            return Transaction(StartTransaction, f);
        }

        [HandleProcessCorruptedStateExceptions]
        public Task<T> Transaction<T>(Func<DbConnection, Task<DbTransaction>> startTransaction, Func<TransactionRunner<TRow>, Task<T>> f, CancellationToken ct = default)
        {
            return WithConnection(async c =>
            {
                await using var transaction = await startTransaction(c);
                var runner = new TransactionRunner<TRow>(this, c, ct);
                try
                {
                    var result = await f(runner);
                    await transaction.CommitAsync(ct);
                    return result;
                }
                catch (Exception)
                {
                    await transaction.RollbackAsync(ct);
                    throw;
                }
            });
        }

        #endregion
    }
}