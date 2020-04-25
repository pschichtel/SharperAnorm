using System;
using System.Collections.Generic;
using NUnit.Framework;
using SharperAnorm;
using Is = NUnit.Framework.Is;

namespace SharperAnormTest
{
    [TestFixture]
    public class QueryTests
    {
        [Test]
        public void SqlWithFormatString()
        {
            var query = Query.Parameterized($"SELECT * FROM x WHERE x.a = {"a"}");
            Console.WriteLine(query);
            Assert.That(query.Statement, Is.EqualTo("SELECT * FROM x WHERE x.a = @var_0"));
            Assert.That(query.Parameters, Is.EqualTo(new Dictionary<string, object>
            {
                ["var_0"] = "a"
            }));
        }
    }
}