using System;
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

        #region No Results

        public Task<int> RunNoResult(Query q)
        {
            return RunNoResult(q, default);
        }

        public async Task<int> RunNoResult(Query q, CancellationToken ct)
        {
            return await WithConnection(async c => await RunNoResultInternal(c, q, ct));
        }

        internal Task<int> RunNoResultInternal(DbConnection c, Query q, CancellationToken ct)
        {
            return RunAction(c, q, cmd => cmd.ExecuteNonQueryAsync(ct));
        }

        #endregion

        #region Single Result, Single Row

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p)
        {
            return RunSingle(q, p, default);
        }

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return WithConnection(c => RunSingleInternal(c, q, p, ct));
        }

        internal async Task<T> RunSingleInternal<T>(DbConnection c, Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return await RunAction(c, q, async cmd =>
            {
                var result = await cmd.ExecuteReaderAsync(ct);

                return await Single(result, p, ct);
            });
        }

        protected abstract Task<T> Single<T>(DbDataReader result, RowParser<T, TRow> parser, CancellationToken ct);

        #endregion

        #region Single Result, Many Rows

        public Task<IQueryResult<T>> Run<T>(Query q, RowParser<T, TRow> p)
        {
            return Run(q, p, default);
        }

        public async Task<IQueryResult<T>> Run<T>(Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            var c = await _connectionProvider();
            return await RunInternal(c, q, p, ct, async () => { await _connectionDisposer(c); });
        }

        internal Task<IQueryResult<T>> RunInternal<T>(DbConnection c, Query q, RowParser<T, TRow> p, CancellationToken ct, Func<Task> onComplete)
        {
            return RunAction(c, q, async cmd =>
            {
                var result = await cmd.ExecuteReaderAsync(ct);

                return Enumerate(result, p, ct, async () =>
                {
                    await result.DisposeAsync();
                    await cmd.DisposeAsync();
                    await onComplete();
                });
            });
        }

        protected abstract IQueryResult<T> Enumerate<T>(DbDataReader result, RowParser<T, TRow> parser, CancellationToken ct, Func<Task> onComplete);

        #endregion

        #region Many Results, Many Rows

        public Task<IQueryResultSet<TRow>> RunMany(Query q)
        {
            return RunMany(q, default);
        }

        public async Task<IQueryResultSet<TRow>> RunMany(Query q, CancellationToken ct)
        {
            var c = await _connectionProvider();
            return await RunManyInternal(c, q, ct, async () => { await _connectionDisposer(c); });
        }

        internal Task<IQueryResultSet<TRow>> RunManyInternal(DbConnection c, Query q, CancellationToken ct, Func<Task> onComplete)
        {
            
            return RunAction(c, q, async cmd =>
            {
                var result = await cmd.ExecuteReaderAsync(ct);

                return EnumerateMany(result, ct, async () =>
                {
                    await result.DisposeAsync();
                    await cmd.DisposeAsync();
                    await onComplete();
                });
            });
        }
        
        protected abstract IQueryResultSet<TRow> EnumerateMany(DbDataReader result, CancellationToken ct, Func<Task> onComplete);

        #endregion

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

        private static async Task<T> RunAction<T>(DbConnection c, Query q, Func<DbCommand, Task<T>> action)
        {
            var cmd = c.CreateCommand();
            cmd.CommandText = q.Statement;
            foreach (var (key, value) in q.Parameters)
            {
                var param = cmd.CreateParameter();
                param.ParameterName = key;
                param.Value = value;
                cmd.Parameters.Add(param);
            }

            return await action(cmd);
        }

        #region Transaction

        public Task<T> Transaction<T>(Func<TransactionRunner<TRow>, Task<T>> f)
        {
            async Task<DbTransaction> StartTransaction(DbConnection c) => await c.BeginTransactionAsync();

            return Transaction(StartTransaction, f);
        }

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