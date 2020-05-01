using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace SharperAnorm
{
    public class DataReaderQueryResultSet : IQueryResultSet<IDataRecord>
    {
        private readonly DbDataReader _reader;
        private readonly CancellationToken _ct;
        private readonly CallOnce<Task> _onComplete;

        public DataReaderQueryResultSet(DbDataReader reader, CancellationToken ct, Func<Task> onComplete)
        {
            _reader = reader;
            _ct = ct;
            _onComplete = new CallOnce<Task>(onComplete);
        }

        public async Task<bool> NextResult()
        {
            var hadMore = await _reader.NextResultAsync(_ct);
            if (!hadMore)
            {
                await DisposeAsync();
            }
            return hadMore;
        }

        public Task<T> SingleRecord<T>(RowParser<T, IDataRecord> parser)
        {
            return DataReaderRunner.ParseSingle(_reader, parser, _ct);
        }

        public IQueryResult<T> AllRecords<T>(RowParser<T, IDataRecord> parser)
        {
            return new DataReaderQueryResult<T>(_reader, parser, _ct, async () => { });
        }

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }

        public async ValueTask DisposeAsync()
        {
            await _onComplete.Call();
        }
    }
}