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
            return _parser(row);
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
        public static RowParser<T, TRow> Safe<T, TRow>(Func<TRow, T> f)
        {
            return new RowParser<T, TRow>(row =>
            {
                try
                {
                    return RowParserResult.Successful(f(row));
                }
                catch (Exception ex)
                {
                    return RowParserResult.Failed<T>(ex);
                }
            });
        }
    }
}