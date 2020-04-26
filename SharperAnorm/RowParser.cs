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
            catch (Exception e)
            {
                return new RowParserError<T>(e);
            }
        }

        public RowParser<TRes, TRow> Map<TRes>(Func<T, TRes> f)
        {
            return new RowParser<TRes, TRow>(_map(f));
        }

        public RowParser<TRes, TRow> FlatMap<TRes>(Func<T, RowParser<TRes, TRow>> f)
        {
            return new RowParser<TRes, TRow>(_flatMap(f));
        }

        public RowParser<(T, TRes), TRow> And<TRes>(RowParser<TRes, TRow> otherParser)
        {
            return new RowParser<(T, TRes), TRow>(row => Parse(row).FlatMap(t => otherParser.Parse(row).Map(tr => (t, tr))));
        }

        protected Func<TRow, RowParserResult<TRes>> _map<TRes>(Func<T, TRes> f)
        {
            return row => Parse(row).Map(f);
        }

        protected Func<TRow, RowParserResult<TRes>> _flatMap<TRes>(Func<T, RowParser<TRes, TRow>> f)
        {
            return row => Parse(row).FlatMap(result => f(result).Parse(row));
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

    public class CellParser<T, TRow> : RowParser<T, TRow>
    {
        public int Column { get; }

        public CellParser(int column, Func<TRow, RowParserResult<T>> parser) : base(parser)
        {
            Column = column;
        }

        public new CellParser<TRes, TRow> Map<TRes>(Func<T, TRes> f)
        {
            return new CellParser<TRes, TRow>(Column, _map(f));
        }

        public new CellParser<TRes, TRow> FlatMap<TRes>(Func<T, RowParser<TRes, TRow>> f)
        {
            return new CellParser<TRes, TRow>(Column, _flatMap(f));
        }
    }

    public static class CellParser
    {
        public static CellParser<T, TRow> Simple<T, TRow>(int column, Func<TRow, T> f)
        {
            return new CellParser<T, TRow>(column, row => RowParserResult.Successful(f(row)));
        }
    } 
}