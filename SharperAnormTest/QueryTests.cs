using System;
using System.Collections.Generic;
using FsCheck;
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

        [FsCheck.NUnit.Property]
        public Property ManualBinding(string name, string value)
        {
            var variable = $"@{name}";
            var q = Query.Plain($"SELECT {variable}")
                .Bind(variable, value);

            return Equals(q.Parameters[variable], value).ToProperty();
        }
    }
}