using System;
using System.Data.Common;
using System.IO;
using System.Linq;
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
        private int _connectionCounter;
        private Func<Task<DbConnection>> _provider;
        private Func<DbConnection, Task> _disposer;

        [SetUp]
        public void Setup()
        {
            _connectionCounter = 0;
            var databasePath = Path.GetTempFileName();
            Console.WriteLine("Database at: " + databasePath);
            _provider = async () =>
            {
                _connectionCounter++;
                var c = new SqliteConnection($"Data Source={databasePath}");
                await c.OpenAsync();
                Console.WriteLine("Connection opened: " + c.GetHashCode());
                return c;
            };
            _disposer = async c =>
            {
                await c.CloseAsync();
                Console.WriteLine("Connection closed: " + c.GetHashCode());
                _connectionCounter--;
            };
        }
        
        [Test]
        public async Task Run()
        {
            var runner = new DataReaderRunner(_provider, _disposer);

            await runner.RunNoResult(Query.Plain("CREATE TABLE test_table (a integer, b integer, c integer)"));
            await runner.RunNoResult(Query.Plain("INSERT INTO test_table (a, b, c) VALUES (1, 2, 3)"));

            var result = await runner.Transaction(async r =>
            {
                var parser = Integer(0).And(Integer(1)).And(Integer(2));
                var ((a, b), c) = await r.RunSingle(Query.Parameterized($"SELECT a, b, c FROM test_table"), parser);
                
                return a *  b * c;
            });
            
            Assert.That(result, Is.EqualTo(6));
        }
        
        [Test]
        public async Task IterateResults()
        {
            var runner = new DataReaderRunner(_provider, _disposer);

            await runner.RunNoResult(Query.Plain("CREATE TABLE test_table (a integer, b integer, c integer)"));
            await runner.RunNoResult(Query.Plain("INSERT INTO test_table (a, b, c) VALUES (1, 2, 3), (2, 3, 4), (5, 6, 7)"));

            var parser = Integer(0).And(Integer(1)).And(Integer(2));
            
            var results = await runner.Run(Query.Parameterized($"SELECT a, b, c FROM test_table"), parser);

            var csSum = results.Select(row =>
            {
                var ((a, b), c) = row;
                return a * b * c;
            }).Sum();

            var sqlSum = await runner.RunSingle(Query.Parameterized($"SELECT SUM(a * b * c) AS sum FROM test_table"), Integer("sum"));
            
            Assert.That(sqlSum, Is.EqualTo(240));
            Assert.That(csSum, Is.EqualTo(sqlSum));
        }
        
        [Test]
        public async Task NullBehavior()
        {
            var runner = new DataReaderRunner(_provider, _disposer);

            var result = await runner.RunSingle(Query.Plain("SELECT null"), Optional(Boolean(0)));
            
            Assert.That(result, Is.EqualTo(Maybe.Nothing<bool>()));
        }
        
        [Test]
        public async Task NamedParser()
        {
            var runner = new DataReaderRunner(_provider, _disposer);

            var result = await runner.RunSingle(Query.Plain("SELECT null as a"), Optional(Boolean("a")));
            
            Assert.That(result, Is.EqualTo(Maybe.Nothing<bool>()));
        }
        
        [Test]
        public async Task IterateResultsPartially()
        {
            var runner = new DataReaderRunner(_provider, _disposer);

            await runner.RunNoResult(Query.Plain("CREATE TABLE test_table (a integer, b integer, c integer)"));
            await runner.RunNoResult(Query.Plain("INSERT INTO test_table (a, b, c) VALUES (1, 2, 3), (2, 3, 4), (5, 6, 7)"));

            var parser = Integer(0).And(Integer(1)).And(Integer(2));

            using (var results = await runner.Run(Query.Plain("SELECT a, b, c FROM test_table"), parser))
            {
                var ((a, b), c) = results.First();
                Assert.That(a * b * c, Is.EqualTo(6));
            }

            Assert.That(_connectionCounter, Is.EqualTo(0));
        }
        
        [Test]
        public async Task IterateResultsLeakedFromTransaction()
        {
            var runner = new DataReaderRunner(_provider, _disposer);

            await runner.RunNoResult(Query.Plain("CREATE TABLE test_table (a integer, b integer, c integer)"));
            await runner.RunNoResult(Query.Plain("INSERT INTO test_table (a, b, c) VALUES (1, 2, 3), (2, 3, 4), (5, 6, 7)"));

            var parser = Integer(0).And(Integer(1)).And(Integer(2));

            var results = await runner.Transaction(r => runner.Run(Query.Plain("SELECT a, b, c FROM test_table"), parser));
            
            var sum = results.Select(row =>
            {
                var ((a, b), c) = row;
                return a * b * c;
            }).Sum();
            
            Assert.That(sum, Is.EqualTo(240));

        }
        
        // Considered non-issue, possibly reevaluate in the future
        // [Test]
        // public async Task IterateResultsInTransaction()
        // {
        //     var runner = new DataReaderRunner(_provider, _disposer);
        //
        //     await runner.RunNoResult(Query.Plain("CREATE TABLE test_table (a integer, b integer, c integer)"));
        //     await runner.RunNoResult(Query.Plain("INSERT INTO test_table (a, b, c) VALUES (1, 2, 3), (2, 3, 4), (5, 6, 7)"));
        //
        //     var parser = Integer(0).And(Integer(1)).And(Integer(2));
        //
        //     await runner.Transaction(async r =>
        //     {
        //         using var results = await runner.Run(Query.Plain("SELECT a, b, c FROM test_table"), parser);
        //         var sum = results.Select(row =>
        //         {
        //             var ((a, b), c) = row;
        //             return a * b * c;
        //         }).Sum();
        //     
        //         Assert.That(sum, Is.EqualTo(240));
        //         return sum;
        //     });
        // }
    }
}