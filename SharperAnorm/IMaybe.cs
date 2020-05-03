using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Runtime.CompilerServices;

namespace SharperAnorm
{
    public interface IMaybe<T>
    {
        public T Value { get; }
        public bool Exists { get; }

        public IMaybe<TR> FlatMap<TR>(Func<T, IMaybe<TR>> f);

        public IMaybe<TR> Map<TR>(Func<T, TR> f);

        public T GetOrElse(T alt);
        public T GetOrElseGet(Func<T> alt);
    }

    public static class Maybe
    {
        public static IMaybe<T> Nothing<T>()
        {
            return SharperAnorm.Nothing.GetInstance<T>();
        }

        public static IMaybe<T> Just<T>(T value)
        {
            return new Just<T>(value);
        }

        public static IMaybe<T> OfNullable<T>(T value) where T: class
        {
            return value == null ? Nothing<T>() : Just(value);
        }
    }

    internal class Just<T> : IMaybe<T>
    {
        public T Value { get; }

        public Just(T value)
        {
            Value = value;
        }

        public bool Exists => true;
        
        public IMaybe<TR> FlatMap<TR>(Func<T, IMaybe<TR>> f)
        {
            return f(Value);
        }

        public IMaybe<TR> Map<TR>(Func<T, TR> f)
        {
            return new Just<TR>(f(Value));
        }

        public T GetOrElse(T alt)
        {
            return Value;
        }

        public T GetOrElseGet(Func<T> alt)
        {
            return Value;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Just<T>) obj);
        }

        public override int GetHashCode()
        {
            return EqualityComparer<T>.Default.GetHashCode(Value);
        }

        public override string ToString()
        {
            return $"{nameof(Just<T>)}({Value})";
        }
    }

    internal class Nothing<T> : IMaybe<T>
    {
        
        public T Value => throw new SqlNullValueException("Value was null");
        public bool Exists => false;

        internal Nothing()
        {}

        public IMaybe<TR> FlatMap<TR>(Func<T, IMaybe<TR>> f)
        {
            return Nothing.GetInstance<TR>();
        }

        public IMaybe<TR> Map<TR>(Func<T, TR> f)
        {
            return Nothing.GetInstance<TR>();
        }

        public T GetOrElse(T alt)
        {
            return alt;
        }

        public T GetOrElseGet(Func<T> alt)
        {
            return alt();
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj);
        }

        public override int GetHashCode()
        {
            return 1;
        }

        public override string ToString()
        {
            return $"{nameof(Nothing<T>)}<{nameof(T)}>";
        }
    }

    internal static class Nothing
    {
        private static readonly Nothing<object> Instance = new Nothing<object>();

        internal static Nothing<TAs> GetInstance<TAs>()
        {
            return Unsafe.As<Nothing<TAs>>(Instance);
        }
    }
}