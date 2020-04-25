using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace SharperAnorm
{
    interface IRunner<TRow>
    {
        Task<IEnumerator<T>> Run<T>(Query q, RowParser<T, TRow> p);
    }
    public abstract class Runner<TRow> : IRunner<TRow>
    {
        private readonly Func<IDbConnection> _connectionProvider;

        protected Runner(Func<IDbConnection> connectionProvider)
        {
            _connectionProvider = connectionProvider;
        }
        
        public async Task<IEnumerator<T>> Run<T>(Query q, RowParser<T, TRow> p)
        {
            using var c = _connectionProvider();
            return await RunWithConnection(c, q, p);
        }

        [HandleProcessCorruptedStateExceptions]
        public async Task<T> Transaction<T>(Func<TransactionRunner<TRow>, Task<T>> f, IsolationLevel isolationLevel)
        {
            using var c = _connectionProvider();
            var transaction = c.BeginTransaction(isolationLevel);
            var runner = new TransactionRunner<TRow>(this, c);
            try
            {
                var result = await f(runner);
                transaction.Commit();
                return result;
            }
            catch (Exception)
            {
                transaction.Rollback();
                throw;
            }
        }

        internal async Task<IEnumerator<T>> RunWithConnection<T>(IDbConnection c, Query q, RowParser<T, TRow> parser)
        {
            await using var cmd = c.CreateCommand() as DbCommand;
            if (cmd == null)
            {
                throw new UnsupportedDriverException();
            }
            
            cmd.CommandText = q.Statement;
            foreach (var (key, value) in q.Parameters)
            {
                var param = cmd.CreateParameter();
                param.ParameterName = key;
                param.Value = value;
            }

            var result = await cmd.ExecuteReaderAsync();

            return Enumerate(result, parser);
        }

        protected abstract IEnumerator<T> Enumerate<T>(DbDataReader result, RowParser<T, TRow> parser);
    }

    public class TransactionRunner<TRow> : IRunner<TRow>
    {
        private Runner<TRow> _runner;
        private IDbConnection _connection;

        public TransactionRunner(Runner<TRow> runner, IDbConnection connection)
        {
            _runner = runner;
            _connection = connection;
        }

        public async Task<IEnumerator<T>> Run<T>(Query q, RowParser<T, TRow> p)
        {
            return await _runner.RunWithConnection(_connection, q, p);
        }
    }
    
    public class UnsupportedDriverException : Exception
    {}

    public class DataReaderRunner : Runner<IDataRecord>
    {
        public DataReaderRunner(Func<IDbConnection> connectionProvider) : base(connectionProvider)
        {
        }

        protected override IEnumerator<T> Enumerate<T>(DbDataReader result, RowParser<T, IDataRecord> parser)
        {
            return new DataReaderParsingEnumerator<T>(result, parser);
        }
    }

    internal class DataReaderParsingEnumerator<T> : IEnumerator<T>
    {
        private readonly IDataReader _reader;
        private readonly RowParser<T, IDataRecord> _parser;

        public DataReaderParsingEnumerator(IDataReader reader, RowParser<T, IDataRecord> parser)
        {
            _reader = reader;
            _parser = parser;
        }

        public bool MoveNext()
        {
            return _reader.Read();
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            _reader.Dispose();
        }

        public T Current => _parser.Parse(_reader).Value;
    }
}