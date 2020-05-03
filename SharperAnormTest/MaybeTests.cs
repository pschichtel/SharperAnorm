
using System;
using System.Data.SqlTypes;
using FsCheck;
using NUnit.Framework;
using SharperAnorm;
using Is = NUnit.Framework.Is;

namespace SharperAnormTest
{
    [TestFixture]
    public class MaybeTests
    {
        [Test]
        public void Just()
        {
            var a = new object();
            IMaybe<object> justA = Maybe.Just(a);
            
            Assert.That(justA.Exists, Is.True);
            Assert.That(a, Is.SameAs(justA.Value));
            
            Assert.That(Maybe.Of(a), Is.EqualTo(justA));
            Assert.That(justA.Equals(null), Is.False);
            Assert.That(Maybe.Just(a), Is.EqualTo(justA));
            Assert.That(Maybe.Just("b"), Is.Not.EqualTo(justA));
            Assert.That(Maybe.Just<object>("b"), Is.Not.EqualTo(justA));
            Assert.That(Maybe.Nothing<string>(), Is.Not.EqualTo(justA));
            
            Assert.That(justA.GetHashCode(), Is.EqualTo(justA.Value.GetHashCode()));
            
            Assert.That(justA.GetOrElse(new object()), Is.SameAs(a));

            bool called = false;
            justA.GetOrElseGet(() =>
            {
                called = true;
                return new object();
            });
            Assert.That(called, Is.False);
        }

        [Test]
        public void Nothing()
        {
            var nothing = Maybe.Nothing<object>();
            Assert.That(nothing.Exists, Is.False);
            Assert.Throws(typeof(SqlNullValueException), () => { Console.WriteLine(nothing.Value); });

            Assert.That(Maybe.Of<object>(null), Is.EqualTo(nothing));
            Assert.That(nothing.Equals(null), Is.False);
            Assert.That(nothing, Is.EqualTo(nothing));
            Assert.That(Maybe.Nothing<object>(), Is.EqualTo(nothing));
            Assert.That(Maybe.Nothing<string>(), Is.EqualTo(nothing));
            Assert.That(Maybe.Just("a"), Is.Not.EqualTo(nothing));
            
            Assert.That(Maybe.Nothing<int>().GetHashCode(), Is.EqualTo(Maybe.Of<string>(null).GetHashCode()));
            
            var alt = new object();
            Assert.That(nothing.GetOrElse(alt), Is.SameAs(alt));

            var called = false;
            var value = nothing.GetOrElseGet(() =>
            {
                called = true;
                return alt;
            });
            Assert.That(called, Is.True);
            Assert.That(value, Is.SameAs(alt));
        }

        [Test]
        public void Nullable()
        {
            int? a = 1;
            Assert.That(Maybe.Of(a), Is.EqualTo(Maybe.Just(1)));
            int? b = null;
            Assert.That(Maybe.Of(b), Is.EqualTo(Maybe.Nothing<int>()));
        }

        #region Functor Laws

        [FsCheck.NUnit.Property]
        public Property MaybePreservesFunctorIdentityMorphisms(object o)
        {
            return Maybe.Of(o).Map(x => x).Equals(Maybe.Of(o))
                .Classify(o == null, "Null => nothing")
                .Classify(o != null, "Non-null => just");
        }

        [FsCheck.NUnit.Property]
        public Property MaybePreservesFunctorMorphismComposition(object o)
        {
            static (T, T) Tuple<T>(T o)
            {
                return (o, o);
            }

            static T Right<X, T>((X, T) tuple)
            {
                return tuple.Item2;
            }
            
            return Maybe.Of(o).Map(x => Right(Tuple(x))).Equals(Maybe.Of(o).Map(Tuple).Map(Right))
                .Classify(o == null, "Null => nothing")
                .Classify(o != null, "Non-null => just");
        }

        [Test]
        public void FlatMapMaybeOfNull()
        {
            Assert.That(Maybe.Of<object>(null).FlatMap(MaybePair).FlatMap(MaybePair), Is.EqualTo(Maybe.Nothing<object>()));
        }

        #endregion

        #region Monad Laws

        [FsCheck.NUnit.Property]
        public Property MaybeHasLeftIdentity(object o)
        {
            return Maybe.Of(o).Map(x => x).Equals(Maybe.Of(o))
                .Classify(o == null, "Null => nothing")
                .Classify(o != null, "Non-null => just");
        }

        [FsCheck.NUnit.Property]
        public Property MaybeHasRightIdentity(object o)
        {
            return Maybe.Of(o).FlatMap(Maybe.Of).Equals(Maybe.Of(o))
                .Classify(o == null, "Null => nothing")
                .Classify(o != null, "Non-null => just");
        }

        [FsCheck.NUnit.Property]
        public Property MaybeIsAssociative(object o)
        {
            IMaybe<object> unit = Maybe.Of<object>(o);
            var a = unit.FlatMap(x => MaybePair(x).FlatMap(MaybePair));
            var b = unit.FlatMap(MaybePair).FlatMap(MaybePair);
            return a.Equals(b)
                .Classify(o == null, "Null => nothing")
                .Classify(o != null, "Non-null => just");
        }

        #endregion
        
        private static IMaybe<(T, T)> MaybePair<T>(T o)
        {
            return Maybe.Just((o, o));
        }
    }
}