using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace SharperAnorm
{
    public class DataReaderQueryResult<T> : IQueryResult<T>
    {
        private readonly DbDataReader _reader;
        private readonly DataReaderParsingEnumerator<T> _enumerator;

        public DataReaderQueryResult(DbDataReader reader, RowParser<T, IDataRecord> parser, CancellationToken ct, Func<Task> onComplete)
        {
            _reader = reader;
            _enumerator = new DataReaderParsingEnumerator<T>(reader, parser, ct, onComplete);
        }

        public int AffectedRows => _reader.RecordsAffected;

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
}