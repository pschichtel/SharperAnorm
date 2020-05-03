
using System;
using System.Data.SqlTypes;
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
            
            Assert.That(Maybe.OfNullable(a), Is.EqualTo(justA));
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

            Assert.That(Maybe.OfNullable<object>(null), Is.EqualTo(nothing));
            Assert.That(nothing.Equals(null), Is.False);
            Assert.That(nothing, Is.EqualTo(nothing));
            Assert.That(Maybe.Nothing<object>(), Is.EqualTo(nothing));
            Assert.That(Maybe.Nothing<string>(), Is.EqualTo(nothing));
            Assert.That(Maybe.Just("a"), Is.Not.EqualTo(nothing));
            
            Assert.That(Maybe.Nothing<int>().GetHashCode(), Is.EqualTo(Maybe.OfNullable<string>(null).GetHashCode()));
            
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
    }
}