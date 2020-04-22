using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace SharperAnorm
{
    public class Runner
    {
        private readonly IDbConnection _connection;

        public Runner(IDbConnection connection)
        {
            _connection = connection;
        }

        public async Task<IEnumerator<T>> Run<T>(Query q, RowParser<T, IDataRecord> parser)
        {
            DbCommand cmd = _connection.CreateCommand() as DbCommand;
            if (cmd == null)
            {
                throw new NotImplementedException();
            }
            cmd.CommandText = q.Statement;
            foreach (var (key, value) in q.Parameters)
            {
                var param = cmd.CreateParameter();
                param.ParameterName = key;
                param.Value = value;
            }

            var result = await cmd.ExecuteReaderAsync();
            
            return new DataReaderParsingEnumerator<T>(result, parser);
        }
    }

    internal class DataReaderParsingEnumerator<T> : IEnumerator<T>
    {
        private readonly IDataReader _reader;
        private readonly RowParser<T, IDataRecord> _parser;

        public DataReaderParsingEnumerator(IDataReader reader, RowParser<T, IDataRecord> parser)
        {
            _reader = reader;
            _parser = parser;
        }

        public bool MoveNext()
        {
            return _reader.Read();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        object IEnumerator.Current
        {
            get { return Current; }
        }

        public void Dispose()
        {
            _reader.Dispose();
        }

        public T Current
        {
            get
            {
                return _parser.Parse(_reader).Value;
            }
        }
    }
}