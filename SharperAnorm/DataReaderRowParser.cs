using System;
using System.Data;

namespace SharperAnorm
{
    public static class DataReaderRowParser
    {
        internal class UnexpectedNullFieldException : Exception
        {
            public static readonly UnexpectedNullFieldException UnexpectedNull = new UnexpectedNullFieldException();

            private UnexpectedNullFieldException()
            {
            }
        }

        private static RowParser<T, IDataRecord> Simple<T>(int col, Func<IDataRecord, T> f)
        {
            return new RowParser<T, IDataRecord>(row =>
            {
                if (row.IsDBNull(col))
                {
                    return RowParserResult.Failed<T>(UnexpectedNullFieldException.UnexpectedNull);
                }

                return RowParserResult.Successful(f(row));
            });
        }

        public static RowParser<IMaybe<T>, IDataRecord> Optional<T>(RowParser<T, IDataRecord> other)
        {
            return new RowParser<IMaybe<T>, IDataRecord>(row =>
            {
                return other.Parse(row).Map(Maybe.Just).Recover(ex =>
                {
                    if (ex == UnexpectedNullFieldException.UnexpectedNull)
                    {
                        return RowParserResult.Successful(Maybe.Nothing<T>());
                    }

                    return RowParserResult.Failed<IMaybe<T>>(ex);
                });
            });
        }

        #region Parse by index

        public static RowParser<string, IDataRecord> String(int colIdx)
        {
            return Simple(colIdx, row => row.GetString(colIdx));
        }

        public static RowParser<int, IDataRecord> Integer(int colIdx)
        {
            return Simple(colIdx, row => row.GetInt32(colIdx));
        }

        public static RowParser<long, IDataRecord> Long(int colIdx)
        {
            return Simple(colIdx, row => row.GetInt64(colIdx));
        }

        public static RowParser<decimal, IDataRecord> Decimal(int colIdx)
        {
            return Simple(colIdx, row => row.GetDecimal(colIdx));
        }

        public static RowParser<bool, IDataRecord> Boolean(int colIdx)
        {
            return Simple(colIdx, row => row.GetBoolean(colIdx));
        }

        public static RowParser<byte, IDataRecord> Byte(int colIdx)
        {
            return Simple(colIdx, row => row.GetByte(colIdx));
        }

        public static RowParser<char, IDataRecord> Char(int colIdx)
        {
            return Simple(colIdx, row => row.GetChar(colIdx));
        }

        public static RowParser<short, IDataRecord> Short(int colIdx)
        {
            return Simple(colIdx, row => row.GetInt16(colIdx));
        }

        public static RowParser<double, IDataRecord> Double(int colIdx)
        {
            return Simple(colIdx, row => row.GetDouble(colIdx));
        }

        public static RowParser<float, IDataRecord> Float(int colIdx)
        {
            return Simple(colIdx, row => row.GetFloat(colIdx));
        }

        public static RowParser<DateTime, IDataRecord> DateTime(int colIdx)
        {
            return Simple(colIdx, row => row.GetDateTime(colIdx));
        }

        public static RowParser<object, IDataRecord> Value(int colIdx)
        {
            return Simple(colIdx, row => row.GetValue(colIdx));
        }

        #endregion

        #region Parse by column name

        public static RowParser<T, IDataRecord> Named<T>(string name, Func<int, RowParser<T, IDataRecord>> byIndexParser)
        {
            return new RowParser<int, IDataRecord>(row => RowParserResult.Successful(row.GetOrdinal(name)))
                .FlatMap(byIndexParser);
        }

        public static RowParser<string, IDataRecord> String(string colName)
        {
            return Named(colName, String);
        }

        public static RowParser<int, IDataRecord> Integer(string colName)
        {
            return Named(colName, Integer);
        }

        public static RowParser<long, IDataRecord> Long(string colName)
        {
            return Named(colName, Long);
        }

        public static RowParser<decimal, IDataRecord> Decimal(string colName)
        {
            return Named(colName, Decimal);
        }

        public static RowParser<bool, IDataRecord> Boolean(string colName)
        {
            return Named(colName, Boolean);
        }

        public static RowParser<byte, IDataRecord> Byte(string colName)
        {
            return Named(colName, Byte);
        }

        public static RowParser<char, IDataRecord> Char(string colName)
        {
            return Named(colName, Char);
        }

        public static RowParser<short, IDataRecord> Short(string colName)
        {
            return Named(colName, Short);
        }

        public static RowParser<double, IDataRecord> Double(string colName)
        {
            return Named(colName, Double);
        }

        public static RowParser<float, IDataRecord> Float(string colName)
        {
            return Named(colName, Float);
        }

        public static RowParser<DateTime, IDataRecord> DateTime(string colName)
        {
            return Named(colName, DateTime);
        }

        public static RowParser<object, IDataRecord> Value(string colName)
        {
            return Named(colName, Value);
        }

        #endregion
    }
}