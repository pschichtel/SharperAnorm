using System;
using System.Data;

namespace SharperAnorm
{
    public static class DataReaderRowParser
    {
        public static RowParser<IMaybe<T>, IDataRecord> Optional<T>(RowParser<T, IDataRecord> other)
        {
            return new RowParser<IMaybe<T>, IDataRecord>(row =>
            {
                try
                {
                    return other.Parse(row).Map(Maybe.Just);
                }
                catch (UnexpectedNullFieldException)
                {
                    return RowParserResult.Successful(Maybe.Nothing<T>());
                }
            });
        }

        #region Simple Parse

        private static RowParser<T, IDataRecord> Simple<T>(Func<IDataRecord, int> getIndex, Func<IDataRecord, int, T> f)
        {
            return new RowParser<T, IDataRecord>(row =>
            {
                int colIdx = getIndex(row);
                if (row.IsDBNull(colIdx))
                {
                    throw UnexpectedNullFieldException.UnexpectedNull;
                }

                return RowParserResult.Successful(f(row, colIdx));
            });
        }

        private static RowParser<string, IDataRecord> SimpleString(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetString(colIdx));
        }

        private static RowParser<int, IDataRecord> SimpleInteger(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetInt32(colIdx));
        }

        private static RowParser<long, IDataRecord> SimpleLong(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetInt64(colIdx));
        }

        private static RowParser<decimal, IDataRecord> SimpleDecimal(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetDecimal(colIdx));
        }

        private static RowParser<bool, IDataRecord> SimpleBoolean(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetBoolean(colIdx));
        }

        private static RowParser<byte, IDataRecord> SimpleByte(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetByte(colIdx));
        }

        private static RowParser<char, IDataRecord> SimpleChar(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetChar(colIdx));
        }

        private static RowParser<short, IDataRecord> SimpleShort(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetInt16(colIdx));
        }

        private static RowParser<double, IDataRecord> SimpleDouble(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetDouble(colIdx));
        }

        private static RowParser<float, IDataRecord> SimpleFloat(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetFloat(colIdx));
        }

        private static RowParser<DateTime, IDataRecord> SimpleDateTime(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetDateTime(colIdx));
        }

        private static RowParser<object, IDataRecord> SimpleValue(Func<IDataRecord, int> getIndex)
        {
            return Simple(getIndex, (row, colIdx) => row.GetValue(colIdx));
        }

        #endregion

        #region Parse by index

        public static RowParser<string, IDataRecord> String(int colIdx)
        {
            return SimpleString(_ => colIdx);
        }

        public static RowParser<int, IDataRecord> Integer(int colIdx)
        {
            return SimpleInteger(_ => colIdx);
        }

        public static RowParser<long, IDataRecord> Long(int colIdx)
        {
            return SimpleLong(_ => colIdx);
        }

        public static RowParser<decimal, IDataRecord> Decimal(int colIdx)
        {
            return SimpleDecimal(_ => colIdx);
        }

        public static RowParser<bool, IDataRecord> Boolean(int colIdx)
        {
            return SimpleBoolean(_ => colIdx);
        }

        public static RowParser<byte, IDataRecord> Byte(int colIdx)
        {
            return SimpleByte(_ => colIdx);
        }

        public static RowParser<char, IDataRecord> Char(int colIdx)
        {
            return SimpleChar(_ => colIdx);
        }

        public static RowParser<short, IDataRecord> Short(int colIdx)
        {
            return SimpleShort(_ => colIdx);
        }

        public static RowParser<double, IDataRecord> Double(int colIdx)
        {
            return SimpleDouble(_ => colIdx);
        }

        public static RowParser<float, IDataRecord> Float(int colIdx)
        {
            return SimpleFloat(_ => colIdx);
        }

        public static RowParser<DateTime, IDataRecord> DateTime(int colIdx)
        {
            return SimpleDateTime(_ => colIdx);
        }

        public static RowParser<object, IDataRecord> Value(int colIdx)
        {
            return SimpleValue(_ => colIdx);
        }

        #endregion

        #region Parse by column name

        public static RowParser<string, IDataRecord> String(string colName)
        {
            return SimpleString(row => row.GetOrdinal(colName));
        }

        public static RowParser<int, IDataRecord> Integer(string colName)
        {
            return SimpleInteger(row => row.GetOrdinal(colName));
        }

        public static RowParser<long, IDataRecord> Long(string colName)
        {
            return SimpleLong(row => row.GetOrdinal(colName));
        }

        public static RowParser<decimal, IDataRecord> Decimal(string colName)
        {
            return SimpleDecimal(row => row.GetOrdinal(colName));
        }

        public static RowParser<bool, IDataRecord> Boolean(string colName)
        {
            return SimpleBoolean(row => row.GetOrdinal(colName));
        }

        public static RowParser<byte, IDataRecord> Byte(string colName)
        {
            return SimpleByte(row => row.GetOrdinal(colName));
        }

        public static RowParser<char, IDataRecord> Char(string colName)
        {
            return SimpleChar(row => row.GetOrdinal(colName));
        }

        public static RowParser<short, IDataRecord> Short(string colName)
        {
            return SimpleShort(row => row.GetOrdinal(colName));
        }

        public static RowParser<double, IDataRecord> Double(string colName)
        {
            return SimpleDouble(row => row.GetOrdinal(colName));
        }

        public static RowParser<float, IDataRecord> Float(string colName)
        {
            return SimpleFloat(row => row.GetOrdinal(colName));
        }

        public static RowParser<DateTime, IDataRecord> DateTime(string colName)
        {
            return SimpleDateTime(row => row.GetOrdinal(colName));
        }

        public static RowParser<object, IDataRecord> Value(string colName)
        {
            return SimpleValue(row => row.GetOrdinal(colName));
        }

        #endregion
    }
}