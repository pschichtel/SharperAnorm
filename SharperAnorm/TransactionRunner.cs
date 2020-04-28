using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace SharperAnorm
{
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

        public Task<IEnumerator<T>> Run<T>(Query q, RowParser<T, TRow> p)
        {
            return _runner.Run(_connection, q, p, _ct);
        }

        public Task RunNoResult(Query q)
        {
            return _runner.RunNoResult(_connection, q, _ct);
        }

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p)
        {
            return _runner.RunSingle(_connection, q, p, _ct);
        }

        public Task<int> RunNonQuery(Query q, RowParser<int, TRow> p)
        {
            return _runner.RunNonQuery(_connection, q, p, _ct);
        }
    }
}