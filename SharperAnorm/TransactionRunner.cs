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

        public Task<int> RunNoResult(Query q)
        {
            return RunNoResult(q, _ct);
        }

        public Task<int> RunNoResult(Query q, CancellationToken ct)
        {
            return _runner.RunNoResultInternal(_connection, q, ct);
        }

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p)
        {
            return RunSingle(q, p, _ct);
        }

        public Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return _runner.RunSingleInternal(_connection, q, p, ct);
        }

        public Task<IQueryResult<T>> Run<T>(Query q, RowParser<T, TRow> p)
        {
            return Run(q, p, _ct);
        }

        public Task<IQueryResult<T>> Run<T>(Query q, RowParser<T, TRow> p, CancellationToken ct)
        {
            return _runner.RunInternal(_connection, q, p, ct, async () => { });
        }

        public Task<IQueryResultSet<TRow>> RunMany(Query q)
        {
            return RunMany(q, _ct);
        }

        public Task<IQueryResultSet<TRow>> RunMany(Query q, CancellationToken ct)
        {
            return _runner.RunManyInternal(_connection, q, ct, async () => { });
        }
    }
}