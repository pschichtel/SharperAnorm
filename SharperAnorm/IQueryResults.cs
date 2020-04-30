using System;
using System.Collections.Generic;

namespace SharperAnorm
{
    public interface IQueryResults<out T> : IEnumerable<T>, IAsyncEnumerable<T>, IDisposable, IAsyncDisposable
    {
    }
}