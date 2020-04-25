using System;
using System.Data;
using Moq;
using NUnit.Framework;
using SharperAnorm;
using static Moq.It;
using Is = NUnit.Framework.Is;

namespace SharperAnormTest
{
    [TestFixture]
    public class DataReaderRowParserTests
    {
        #region By index

        [Test]
        public void GetStringByIndex()
        {
            const string expected = "Test";
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetString(0)).Returns(expected);

            Assert.That(DataReaderRowParser.String(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetIntegerByIndex()
        {
            const int expected = 123456789;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetInt32(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Integer(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetLongByIndex()
        {
            const long expected = 1234567890123456;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetInt64(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Long(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetDecimalByIndex()
        {
            const decimal expected = 12345679.0987m;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetDecimal(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Decimal(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetBooleanByIndex()
        {
            const bool expected = true;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetBoolean(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Boolean(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetByteByIndex()
        {
            const byte expected = 123;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetByte(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Byte(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetCharByIndex()
        {
            const char expected = '@';
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetChar(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Char(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetShortByIndex()
        {
            const short expected = 12345;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetInt16(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Short(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetDoubleByIndex()
        {
            const double expected = 12345679.0987;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetDouble(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Double(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetFloatByIndex()
        {
            const float expected = 12345679.0987f;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetFloat(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Float(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetValueByIndex()
        {
            var expected = (12345679.0987m, "Test");
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetValue(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Value(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetDateTimeByIndex()
        {
            var expected = DateTime.Now;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetDateTime(0)).Returns(expected);

            Assert.That(DataReaderRowParser.DateTime(0).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        #endregion

        #region By column name

        [Test]
        public void GetStringByName()
        {
            const string name = "string";
            const string expected = "Test";
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetString(0)).Returns(expected);

            Assert.That(DataReaderRowParser.String(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetIntegerByName()
        {
            const string name = "int";
            const int expected = 123456789;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetInt32(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Integer(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetLongByName()
        {
            const string name = "long";
            const long expected = 1234567890123456;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetInt64(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Long(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetDecimalByName()
        {
            const string name = "decimal";
            const decimal expected = 12345679.0987m;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetDecimal(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Decimal(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetBooleanByName()
        {
            const string name = "bool";
            const bool expected = true;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetBoolean(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Boolean(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetByteByName()
        {
            const string name = "byte";
            const byte expected = 123;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetByte(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Byte(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetCharByName()
        {
            const string name = "char";
            const char expected = '@';
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetChar(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Char(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetShortByName()
        {
            const string name = "short";
            const short expected = 12345;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetInt16(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Short(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetDoubleByName()
        {
            const string name = "double";
            const double expected = 12345679.0987;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetDouble(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Double(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetFloatByName()
        {
            const string name = "float";
            const float expected = 12345679.0987f;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetFloat(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Float(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetDateTimeByName()
        {
            const string name = "datetime";
            var expected = DateTime.Now;
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetDateTime(0)).Returns(expected);

            Assert.That(DataReaderRowParser.DateTime(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        [Test]
        public void GetValueByName()
        {
            const string name = "value";
            var expected = (12345679.0987m, "Test");
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(name)).Returns(0);
            mock.Setup(dr => dr.GetValue(0)).Returns(expected);

            Assert.That(DataReaderRowParser.Value(name).Map(t => t).Parse(mock.Object).Value, Is.EqualTo(expected));
        }

        #endregion

        [Test]
        public void IndexNotFound()
        {
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetString(IsAny<int>())).Throws(new IndexOutOfRangeException());

            Assert.That(DataReaderRowParser.String(1).Map(t => t).Parse(mock.Object).Successful, Is.False);
        }

        [Test]
        public void ColumnNameNotFound()
        {
            var mock = new Mock<IDataRecord>();
            mock.Setup(dr => dr.GetOrdinal(IsAny<string>())).Throws(new IndexOutOfRangeException());

            Assert.That(DataReaderRowParser.String("some_column").Parse(mock.Object).Successful, Is.False);
        }
    }
}