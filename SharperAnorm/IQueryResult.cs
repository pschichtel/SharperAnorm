using System;
using System.Collections.Generic;

namespace SharperAnorm
{
    public interface IQueryResult<out T> : IEnumerable<T>, IAsyncEnumerable<T>, IDisposable, IAsyncDisposable
    {
        int AffectedRows { get; }
    }
}