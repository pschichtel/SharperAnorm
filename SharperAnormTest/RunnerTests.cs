using System;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using NUnit.Framework;
using SharperAnorm;
using static SharperAnorm.DataReaderRowParser;

namespace SharperAnormTest
{
    [TestFixture]
    public class RunnerTests
    {
        private DbConnection _connection;
        
        [SetUp]
        public void Setup()
        {
            var databasePath = Path.GetTempFileName();
            Console.WriteLine("Database at: " + databasePath);
            _connection = new SqliteConnection($"Data Source={databasePath}");
            _connection.Open();
        }
        
        [Test]
        public async Task Run()
        {
            var runner = new DataReaderRunner(async () => _connection, async (c) => { });

            await runner.RunNoResult(Query.Plain("CREATE TABLE test_table (a integer, b integer, c integer)"));
            await runner.RunNoResult(Query.Plain("INSERT INTO test_table (a, b, c) VALUES (1, 2, 3)"));

            var a = await runner.Transaction(async r =>
            {
                var parser = Integer(0).And(Integer(1)).And(Integer(2));
                var ((a, b), c) = await r.RunSingle(Query.Parameterized($"SELECT a, b, c FROM test_table"), parser);
                
                return a *  b * c;
            });
            
            Assert.That(a, Is.EqualTo(6));
        }
        
        [Test]
        public async Task NullBehavior()
        {
            var runner = new DataReaderRunner(async () => _connection, async (c) => { });

            var result = await runner.RunSingle(Query.Plain("SELECT null"), Optional(Boolean(0)));
            
            Assert.That(result, Is.EqualTo(Maybe.Nothing<bool>()));
        }
    }
}