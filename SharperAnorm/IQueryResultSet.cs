using System;
using System.Threading.Tasks;

namespace SharperAnorm
{
    public interface IQueryResultSet<TRow> : IDisposable, IAsyncDisposable
    {
        Task<bool> NextResult();
        Task<T> SingleRecord<T>(RowParser<T, TRow> parser);
        IQueryResult<T> AllRecords<T>(RowParser<T, TRow> parser);
    }
}