using System;

namespace SharperAnorm
{
    public abstract class RowParserResult<T>
    {
        internal RowParserResult()
        {
        }

        public abstract T Value { get; }
        public abstract bool Successful { get; }

        public abstract RowParserResult<TR> FlatMap<TR>(Func<T, RowParserResult<TR>> f);

        public abstract RowParserResult<TR> Map<TR>(Func<T, TR> f);

        public abstract TR Fold<TR>(Func<Exception, TR> onError, Func<T, TR> onSuccess);
    }

    internal class RowParserSuccess<T> : RowParserResult<T>
    {
        public override T Value { get; }

        public override bool Successful
        {
            get { return true; }
        }

        public RowParserSuccess(T value)
        {
            Value = value;
        }

        public override RowParserResult<TR> FlatMap<TR>(Func<T, RowParserResult<TR>> f)
        {
            return f(Value);
        }

        public override RowParserResult<TR> Map<TR>(Func<T, TR> f)
        {
            return new RowParserSuccess<TR>(f(Value));
        }

        public override TR Fold<TR>(Func<Exception, TR> onError, Func<T, TR> onSuccess)
        {
            return onSuccess(Value);
        }
    }

    internal class RowParserError<T> : RowParserResult<T>
    {
        private readonly Exception _error;

        public override T Value
        {
            get { throw _error; }
        }

        public override bool Successful
        {
            get { return false; }
        }

        public RowParserError(Exception error)
        {
            _error = error;
        }

        public override RowParserResult<TR> FlatMap<TR>(Func<T, RowParserResult<TR>> f)
        {
            return new RowParserError<TR>(_error);
        }

        public override RowParserResult<TR> Map<TR>(Func<T, TR> f)
        {
            return new RowParserError<TR>(_error);
        }

        public override TR Fold<TR>(Func<Exception, TR> onError, Func<T, TR> onSuccess)
        {
            return onError(_error);
        }
    }

    public static class RowParserResult
    {
        public static RowParserResult<T> Successful<T>(T value)
        {
            return new RowParserSuccess<T>(value);
        }

        public static RowParserResult<T> Failed<T>(Exception error)
        {
            return new RowParserError<T>(error);
        }
    }
}