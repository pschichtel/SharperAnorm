using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace SharperAnorm
{
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
            throw new NotSupportedException();
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            _reader.Dispose();
        }

        public T Current => _parser.Parse(_reader).Value;
    }
}