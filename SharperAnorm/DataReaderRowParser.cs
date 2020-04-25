using System;
using System.Data;

namespace SharperAnorm
{
    public static class DataReaderRowParser
    {
        private static RowParser<T, IDataRecord> Safe<T>(Func<IDataRecord, T> f)
        {
            return RowParser.Safe(f);
        }

        #region Parse by index

        public static RowParser<string, IDataRecord> String(int colIdx)
        {
            return Safe(row => row.GetString(colIdx));
        }

        public static RowParser<int, IDataRecord> Integer(int colIdx)
        {
            return Safe(row => row.GetInt32(colIdx));
        }

        public static RowParser<long, IDataRecord> Long(int colIdx)
        {
            return Safe(row => row.GetInt64(colIdx));
        }

        public static RowParser<decimal, IDataRecord> Decimal(int colIdx)
        {
            return Safe(row => row.GetDecimal(colIdx));
        }

        public static RowParser<bool, IDataRecord> Boolean(int colIdx)
        {
            return Safe(row => row.GetBoolean(colIdx));
        }

        public static RowParser<byte, IDataRecord> Byte(int colIdx)
        {
            return Safe(row => row.GetByte(colIdx));
        }

        public static RowParser<char, IDataRecord> Char(int colIdx)
        {
            return Safe(row => row.GetChar(colIdx));
        }

        public static RowParser<short, IDataRecord> Short(int colIdx)
        {
            return Safe(row => row.GetInt16(colIdx));
        }

        public static RowParser<double, IDataRecord> Double(int colIdx)
        {
            return Safe(row => row.GetDouble(colIdx));
        }

        public static RowParser<float, IDataRecord> Float(int colIdx)
        {
            return Safe(row => row.GetFloat(colIdx));
        }

        public static RowParser<DateTime, IDataRecord> DateTime(int colIdx)
        {
            return Safe(row => row.GetDateTime(colIdx));
        }

        public static RowParser<object, IDataRecord> Value(int colIdx)
        {
            return Safe(row => row.GetValue(colIdx));
        }

        #endregion

        #region Parse by column name

        public static RowParser<string, IDataRecord> String(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetString(colIdx);
            });
        }

        public static RowParser<int, IDataRecord> Integer(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetInt32(colIdx);
            });
        }

        public static RowParser<long, IDataRecord> Long(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetInt64(colIdx);
            });
        }

        public static RowParser<decimal, IDataRecord> Decimal(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetDecimal(colIdx);
            });
        }

        public static RowParser<bool, IDataRecord> Boolean(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetBoolean(colIdx);
            });
        }

        public static RowParser<byte, IDataRecord> Byte(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetByte(colIdx);
            });
        }

        public static RowParser<char, IDataRecord> Char(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetChar(colIdx);
            });
        }

        public static RowParser<short, IDataRecord> Short(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetInt16(colIdx);
            });
        }

        public static RowParser<double, IDataRecord> Double(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetDouble(colIdx);
            });
        }

        public static RowParser<float, IDataRecord> Float(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetFloat(colIdx);
            });
        }

        public static RowParser<DateTime, IDataRecord> DateTime(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetDateTime(colIdx);
            });
        }

        public static RowParser<object, IDataRecord> Value(string colName)
        {
            return Safe(row =>
            {
                var colIdx = row.GetOrdinal(colName);
                return row.GetValue(colIdx);
            });
        }

        #endregion
    }
}