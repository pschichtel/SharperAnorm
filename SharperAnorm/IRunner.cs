using System.Collections.Generic;
using System.Threading.Tasks;

namespace SharperAnorm
{
    public interface IRunner<TRow>
    {
        Task<IEnumerable<T>> Run<T>(Query q, RowParser<T, TRow> p);
        Task RunNoResult(Query q);
        Task<T> RunSingle<T>(Query q, RowParser<T, TRow> p);
    }
}