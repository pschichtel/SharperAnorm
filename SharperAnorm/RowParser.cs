using System;

namespace SharperAnorm
{
    public class RowParser<T, TRow>
    {
        private readonly Func<TRow, RowParserResult<T>> _parser;

        public RowParser(Func<TRow, RowParserResult<T>> parser)
        {
            _parser = parser;
        }

        public RowParserResult<T> Parse(TRow row)
        {
            try
            {
                return _parser(row);
            }
            catch (UnexpectedNullFieldException)
            {
                throw;
            }
            catch (Exception ex)
            {
                return RowParserResult.Failed<T>(ex);
            }
        }

        public RowParser<TRes, TRow> Map<TRes>(Func<T, TRes> f)
        {
            return new RowParser<TRes, TRow>(row => Parse(row).Map(f));
        }

        public RowParser<TRes, TRow> FlatMap<TRes>(Func<T, RowParser<TRes, TRow>> f)
        {
            return new RowParser<TRes, TRow>(row => Parse(row).FlatMap(result => f(result).Parse(row)));
        }

        public RowParser<(T, TRes), TRow> And<TRes>(RowParser<TRes, TRow> otherParser)
        {
            return new RowParser<(T, TRes), TRow>(row => Parse(row).FlatMap(t => otherParser.Parse(row).Map(tr => (t, tr))));
        }
    }

    public static class RowParser
    {
        public static RowParser<T, TRow> Simple<T, TRow>(Func<TRow, T> f)
        {
            return new RowParser<T, TRow>(row => RowParserResult.Successful(f(row)));
        }

        public static RowParser<T, TRow> Constant<T, TRow>(T value)
        {
            return new RowParser<T, TRow>(_ => RowParserResult.Successful(value));
        }
    }
    
    public class UnexpectedNullFieldException : Exception
    {
        public static readonly UnexpectedNullFieldException UnexpectedNull = new UnexpectedNullFieldException();

        private UnexpectedNullFieldException()
        {}
    }
}