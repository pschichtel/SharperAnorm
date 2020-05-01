using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SharperAnorm
{
    public interface IRunner<TRow>
    {
        Task<int> RunNoResult(Query q);
        Task<int> RunNoResult(Query q, CancellationToken ct);
        Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p);
        Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p, CancellationToken ct);
        Task<IQueryResult<T>> Run<T>(Query q, RowParser<T, TRow> p);
        Task<IQueryResult<T>> Run<T>(Query q, RowParser<T, TRow> p, CancellationToken ct);
        Task<IQueryResultSet<TRow>> RunMany(Query q);
        Task<IQueryResultSet<TRow>> RunMany(Query q, CancellationToken ct);
    }
}