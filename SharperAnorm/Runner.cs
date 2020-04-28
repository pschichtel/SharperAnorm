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

        private async Task<T> WithConnection<T>(Func<Rc<DbConnection>, Task<T>> f)
        {
            var c = await _connectionProvider();
            return await f(new Rc<DbConnection>(c, _connectionDisposer));
        }

        private async Task WithConnection(Func<Rc<DbConnection>, Task> f)
        {
            var c = await _connectionProvider();
            await f(new Rc<DbConnection>(c, _connectionDisposer));
        }

        #region No Result

        public Task RunNoResult(Query q)
        {
            return RunNoResult(q, default);
        }

        public async Task RunNoResult(Query q, CancellationToken ct)
        {
            await WithConnection(async c =>
            {
                await RunNoResult(c, q, ct);
            });
        }

        internal Task RunNoResult(Rc<DbConnection> c, Query q, CancellationToken ct)
        {
            return RunAction(c, q, async cmd =>
            {
                try
                {
                    return await cmd.ExecuteNonQueryAsync(ct);
                }
                finally
                {
                    c.Dispose();
                }
            });
        }

        #endregion

        #region Single Result

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p)
        {
            return RunSingle(q, p, default);
        }

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return WithConnection(c =>
            {
                var cRef = c.Clone();
                return RunAction(cRef, q, async cmd =>
                {
                    var result = await cmd.ExecuteReaderAsync(ct);

                    return await Single(result, p, ct);
                });
            });
        }

        internal async Task<T> RunSingle<T>(Rc<DbConnection> c, Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return await RunAction(c.Clone(), q, async cmd =>
            {
                var result = await cmd.ExecuteReaderAsync(ct);

                try
                {
                    var parsedResult = await Single(result, p, ct);
                    return parsedResult;
                }
                finally
                {
                    c.Dispose();
                }
            });
        }

        protected abstract Task<T> Single<T>(DbDataReader result, RowParser<T, TRow> parser, CancellationToken ct);

        #endregion

        #region Many Results

        public async Task<IEnumerable<T>> Run<T>(Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return await WithConnection(c => Run(c, q, p, ct));
        }

        public Task<IEnumerable<T>> Run<T>(Query q, RowParser<T, TRow> p)
        {
            return Run(q, p, default);
        }

        internal Task<IEnumerable<T>> Run<T>(Rc<DbConnection> c, Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return RunAction(c, q, async cmd =>
            {
                var result = await cmd.ExecuteReaderAsync(ct);

                return Enumerate(c, cmd, result, p);
            });
        }

        protected abstract IEnumerable<T> Enumerate<T>(Rc<DbConnection> connection, DbCommand cmd, DbDataReader result, RowParser<T, TRow> parser);

        #endregion

        private static async Task<T> RunAction<T>(Rc<DbConnection> c, Query q, Func<DbCommand, Task<T>> action)
        {
            var cmd = c.Value.CreateCommand();
            cmd.CommandText = q.Statement;
            foreach (var (key, value) in q.Parameters)
            {
                var param = cmd.CreateParameter();
                param.ParameterName = key;
                param.Value = value;
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
                await using var transaction = await startTransaction(c.Value);
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

    public class TransactionRunner<TRow> : IRunner<TRow>
    {
        private readonly Runner<TRow> _runner;
        private readonly Rc<DbConnection> _connection;
        private readonly CancellationToken _ct;

        internal TransactionRunner(Runner<TRow> runner, Rc<DbConnection> connection, CancellationToken ct)
        {
            _runner = runner;
            _connection = connection;
            _ct = ct;
        }

        public Task<IEnumerable<T>> Run<T>(Query q, RowParser<T, TRow> p)
        {
            return _runner.Run(_connection.Clone(), q, p, _ct);
        }

        public Task RunNoResult(Query q)
        {
            return _runner.RunNoResult(_connection.Clone(), q, _ct);
        }

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p)
        {
            return _runner.RunSingle(_connection.Clone(), q, p, _ct);
        }
    }
    
    public class DataReaderRunner : Runner<IDataRecord>
    {
        public DataReaderRunner(Func<Task<DbConnection>> connectionProvider, Func<DbConnection, Task> connectionDisposer) : base(connectionProvider, connectionDisposer)
        {
        }

        protected override IEnumerable<T> Enumerate<T>(Rc<DbConnection> connection, DbCommand cmd, DbDataReader result, RowParser<T, IDataRecord> parser)
        {
            try
            {
                while (result.Read())
                {
                    yield return parser.Parse(result).Value;
                }
            }
            finally
            {
                result.Dispose();
                cmd.Dispose();
                connection.Dispose();
            }

        }

        protected override async Task<T> Single<T>(DbDataReader result, RowParser<T, IDataRecord> parser, CancellationToken ct)
        {
            var success = await result.ReadAsync(ct);
            if (!success)
            {
                throw new DataException("Expected a single value, but got nothing!"); 
            }

            var parseResult = parser.Parse(result).Value;

            success = await result.ReadAsync(ct);
            if (success)
            {
                throw new DataException("Expected a single value, but got more than that!");
            }

            return parseResult;
        }
    }
}