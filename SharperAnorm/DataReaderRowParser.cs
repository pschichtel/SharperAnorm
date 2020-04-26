using System;
using System.Data;

namespace SharperAnorm
{
    public static class DataReaderRowParser
    {
        private static CellParser<T, IDataRecord> Simple<T>(int column, Func<IDataRecord, T> f)
        {
            return CellParser.Simple(column, f);
        }

        public static RowParser<IMaybe<T>, IDataRecord> Optional<T>(CellParser<T, IDataRecord> other)
        {
            return new RowParser<IMaybe<T>, IDataRecord>(row =>
            {
                if (row.IsDBNull(other.Column))
                {
                    return RowParserResult.Successful(Maybe.Nothing<T>());
                }

                return other.Parse(row).Map(Maybe.Just);
            });
        }

        #region Parse by index

        public static CellParser<string, IDataRecord> String(int colIdx)
        {
            return Simple(colIdx, row => row.GetString(colIdx));
        }

        public static CellParser<int, IDataRecord> Integer(int colIdx)
        {
            return Simple(colIdx, row => row.GetInt32(colIdx));
        }

        public static CellParser<long, IDataRecord> Long(int colIdx)
        {
            return Simple(colIdx, row => row.GetInt64(colIdx));
        }

        public static CellParser<decimal, IDataRecord> Decimal(int colIdx)
        {
            return Simple(colIdx, row => row.GetDecimal(colIdx));
        }

        public static CellParser<bool, IDataRecord> Boolean(int colIdx)
        {
            return Simple(colIdx, row => row.GetBoolean(colIdx));
        }

        public static CellParser<byte, IDataRecord> Byte(int colIdx)
        {
            return Simple(colIdx, row => row.GetByte(colIdx));
        }

        public static CellParser<char, IDataRecord> Char(int colIdx)
        {
            return Simple(colIdx, row => row.GetChar(colIdx));
        }

        public static CellParser<short, IDataRecord> Short(int colIdx)
        {
            return Simple(colIdx, row => row.GetInt16(colIdx));
        }

        public static CellParser<double, IDataRecord> Double(int colIdx)
        {
            return Simple(colIdx, row => row.GetDouble(colIdx));
        }

        public static CellParser<float, IDataRecord> Float(int colIdx)
        {
            return Simple(colIdx, row => row.GetFloat(colIdx));
        }

        public static CellParser<DateTime, IDataRecord> DateTime(int colIdx)
        {
            return Simple(colIdx, row => row.GetDateTime(colIdx));
        }

        public static CellParser<object, IDataRecord> Value(int colIdx)
        {
            return Simple(colIdx, row => row.GetValue(colIdx));
        }

        #endregion

        #region Parse by column name

        public static CellParser<T, IDataRecord> Named<T>(string name, Func<int, CellParser<T, IDataRecord>> byIndexParser)
        {
            return Simple(-1, row => row.GetOrdinal(name)).FlatMap(byIndexParser);
        }

        public static CellParser<string, IDataRecord> String(string colName)
        {
            return Named(colName, String);
        }

        public static CellParser<int, IDataRecord> Integer(string colName)
        {
            return Named(colName, Integer);
        }

        public static CellParser<long, IDataRecord> Long(string colName)
        {
            return Named(colName, Long);
        }

        public static CellParser<decimal, IDataRecord> Decimal(string colName)
        {
            return Named(colName, Decimal);
        }

        public static CellParser<bool, IDataRecord> Boolean(string colName)
        {
            return Named(colName, Boolean);
        }

        public static CellParser<byte, IDataRecord> Byte(string colName)
        {
            return Named(colName, Byte);
        }

        public static CellParser<char, IDataRecord> Char(string colName)
        {
            return Named(colName, Char);
        }

        public static CellParser<short, IDataRecord> Short(string colName)
        {
            return Named(colName, Short);
        }

        public static CellParser<double, IDataRecord> Double(string colName)
        {
            return Named(colName, Double);
        }

        public static CellParser<float, IDataRecord> Float(string colName)
        {
            return Named(colName, Float);
        }

        public static CellParser<DateTime, IDataRecord> DateTime(string colName)
        {
            return Named(colName, DateTime);
        }

        public static CellParser<object, IDataRecord> Value(string colName)
        {
            return Named(colName, Value);
        }

        #endregion
    }
}