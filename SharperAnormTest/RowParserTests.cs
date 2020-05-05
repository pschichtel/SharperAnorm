using FsCheck;
using NUnit.Framework;
using SharperAnorm;

namespace SharperAnormTest
{
    [TestFixture]
    public class RowParserTests
    {
        [FsCheck.NUnit.Property]
        public Property ConstantParser(int i)
        {
            return (RowParser.Constant<int, string>(i).Parse("").Value == i).ToProperty();
        }
        
        [FsCheck.NUnit.Property]
        public Property SimpleParser(int i)
        {
            return (RowParser.Simple<int, int>(s => s).Parse(i).Value == i).ToProperty();
        }
    }
}