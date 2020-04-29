using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
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

        internal Task RunNoResult(DbConnection c, Query q, CancellationToken ct)
        {
            return RunAction(c, q, cmd => cmd.ExecuteNonQueryAsync(ct));
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

                var parsedResult = await Single(result, p, ct);
                return parsedResult;
            });
        }

        protected abstract Task<T> Single<T>(DbDataReader result, RowParser<T, TRow> parser, CancellationToken ct);

        #endregion

        #region Many Results

        public Task<IQueryResults<T>> Run<T>(Query q, RowParser<T, TRow> p)
        {
            return Run(q, p, default);
        }

        public async Task<IQueryResults<T>> Run<T>(Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            var c = await _connectionProvider();
            return await Run(c, q, p, ct, async () =>
            {
                await _connectionDisposer(c);
            });
        }

        internal Task<IQueryResults<T>> Run<T>(DbConnection c, Query q, RowParser<T, TRow> p, CancellationToken ct, Func<Task> onComplete)
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

        protected abstract IQueryResults<T> Enumerate<T>(DbDataReader result, RowParser<T, TRow> parser, CancellationToken ct, Func<Task> onComplete);

        #endregion

        private static async Task<T> RunAction<T>(DbConnection c, Query q, Func<DbCommand, Task<T>> action)
        {
            var cmd = c.CreateCommand();
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

    public class TransactionRunner<TRow> : IRunner<TRow>
    {
        private readonly Runner<TRow> _runner;
        private readonly DbConnection _connection;
        private readonly CancellationToken _ct;

        internal TransactionRunner(Runner<TRow> runner, DbConnection connection, CancellationToken ct)
        {
            _runner = runner;
            _connection = connection;
            _ct = ct;
        }

        public Task<IQueryResults<T>> Run<T>(Query q, RowParser<T, TRow> p)
        {
            return _runner.Run(_connection, q, p, _ct, async () => {});
        }

        public Task RunNoResult(Query q)
        {
            return _runner.RunNoResult(_connection, q, _ct);
        }

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p)
        {
            return _runner.RunSingle(_connection, q, p, _ct);
        }
    }
    
    public class DataReaderRunner : Runner<IDataRecord>
    {
        public DataReaderRunner(Func<Task<DbConnection>> connectionProvider, Func<DbConnection, Task> connectionDisposer) : base(connectionProvider, connectionDisposer)
        {
        }

        protected override IQueryResults<T> Enumerate<T>(DbDataReader result, RowParser<T, IDataRecord> parser, CancellationToken ct, Func<Task> onComplete)
        {
            return new DataReaderQueryResults<T>(result, parser, ct, onComplete);

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

    public interface IQueryResults<out T> : IEnumerable<T>, IAsyncEnumerable<T>, IDisposable, IAsyncDisposable
    {
        
    }
    
    public class DataReaderQueryResults<T> : IQueryResults<T>
    {
        private readonly DataReaderParsingEnumerator<T> _enumerator;

        public DataReaderQueryResults(DbDataReader reader, RowParser<T, IDataRecord> parser, CancellationToken ct, Func<Task> onComplete)
        {
            _enumerator = new DataReaderParsingEnumerator<T>(reader, parser, ct, onComplete);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _enumerator;
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = new CancellationToken())
        {
            return _enumerator;
        }

        public void Dispose()
        {
            _enumerator.Dispose();
        }

        public ValueTask DisposeAsync()
        {
            return _enumerator.DisposeAsync();
        }
    }
    
    public class DataReaderParsingEnumerator<T> : IEnumerator<T>, IAsyncEnumerator<T>
    {
        private readonly DbDataReader _reader;
        private readonly RowParser<T, IDataRecord> _parser;
        private readonly CancellationToken _ct;
        private readonly Func<Task> _onComplete;
        private bool _completed = false;

        public DataReaderParsingEnumerator(DbDataReader reader, RowParser<T, IDataRecord> parser, CancellationToken ct, Func<Task> onComplete)
        {
            _reader = reader;
            _parser = parser;
            _ct = ct;
            _onComplete = onComplete;
        }

        public bool MoveNext()
        {
            if (!_reader.Read())
            {
                Dispose();
                return false;
            }
            return true;
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            if (!await _reader.ReadAsync(_ct))
            {
                await DisposeAsync();
                return false;
            }
            return true;
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public async ValueTask DisposeAsync()
        {
            if (!_completed)
            {
                _completed = true;
                await _onComplete();
            }
        }

        public T Current => _parser.Parse(_reader).Value;
    }
}