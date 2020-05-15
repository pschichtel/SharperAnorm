using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace SharperAnorm
{
    public class DataReaderParsingEnumerator<T> : IEnumerator<T>, IAsyncEnumerator<T>
    {
        private readonly DbDataReader _reader;
        private readonly RowParser<T, IDataRecord> _parser;
        private readonly CancellationToken _ct;
        private readonly CallOnce<Task> _onComplete;

        public DataReaderParsingEnumerator(DbDataReader reader, RowParser<T, IDataRecord> parser, CancellationToken ct, Func<Task> onComplete)
        {
            _reader = reader;
            _parser = parser;
            _ct = ct;
            _onComplete = new CallOnce<Task>(onComplete);
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

        object? IEnumerator.Current => Current;

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }

        public async ValueTask DisposeAsync()
        {
            await _onComplete.Call();
        }

        public T Current => _parser.Parse(_reader).Value;
    }
}