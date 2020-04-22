using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace SharperAnorm
{
    public static class DataReaderRowParser
    {
        private static RowParser<T, IDataRecord> Safe<T>(Func<IDataRecord, T> f)
        {
            return RowParser.Safe(f);
        }
        
        public static RowParser<string, IDataRecord> Str(int colIdx)
        {
            return Safe(row => row.GetString(colIdx));
        }
        
        public static RowParser<int, IDataRecord> Int(int colIdx)
        {
            return Safe(row => row.GetInt32(colIdx));
        }
        
        public static RowParser<decimal, IDataRecord> Decimal(int colIdx)
        {
            return Safe(row => row.GetDecimal(colIdx));
        }
    }
}