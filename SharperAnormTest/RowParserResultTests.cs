using System;
using NUnit.Framework;
using SharperAnorm;

namespace SharperAnormTest
{
    [TestFixture]
    public class RowParserResultTests
    {
        [Test]
        public void Successful()
        {
            var o = new object();
            var result = RowParserResult.Successful(o);
            Assert.True(result.Successful);
            Assert.AreSame(result.Value, o);

            Assert.AreSame(result.Fold(e => e, r => r), o);
            Assert.AreSame(result.Recover(e => RowParserResult.Successful<object>(null)), result);
        }
        
        [Test]
        public void Failed()
        {
            var ex = new Exception();
            var result = RowParserResult.Failed<object>(ex);
            Assert.False(result.Successful);
            Assert.AreSame(Assert.Throws<Exception>(() => Console.WriteLine(result.Value)), ex);

            Assert.AreSame(result.Fold(e => e, r => r), ex);
            Assert.AreSame(result.Recover(RowParserResult.Successful<object>).Value, ex);
        }

        [Test]
        public void Composition()
        {
            var a = new object();
            var b = new object();
            var ex = new Exception();
            var good = RowParserResult.Successful(a);
            var bad = RowParserResult.Failed<object>(ex);
            
            Assert.AreSame(good.Map(_ => b).Value, b);
            Assert.AreSame(good.FlatMap(_ => RowParserResult.Successful(b)).Value, b);
            
            Assert.AreSame(Assert.Throws<Exception>(() => Console.WriteLine(bad.Map(_ => b).Value)), ex);
            Assert.AreSame(Assert.Throws<Exception>(() => Console.WriteLine(bad.FlatMap(_ => RowParserResult.Successful(b)).Value)), ex);
        }
    }
}