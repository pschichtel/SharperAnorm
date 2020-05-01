using System;
using System.Collections.Generic;
using System.Linq;

namespace SharperAnorm
{
    public class Query
    {
        public string Statement { get; }
        public IDictionary<string, object> Parameters { get; }

        private Query(string statement, IDictionary<string, object> parameters)
        {
            Statement = statement;
            Parameters = parameters;
        }

        public Query Bind(string name, object value)
        {
            var newParameters = new Dictionary<string, object>(Parameters) {[name] = value};
            return new Query(Statement, newParameters);
        }

        public override string ToString()
        {
            return "[" + Statement + "] with " + string.Join(", ", Parameters.Select(e => e.Key + " = " + e.Value));
        }

        public static Query Plain(string statement)
        {
            return new Query(statement, new Dictionary<string, object>());
        }

        public static Query Parameterized(FormattableString statement)
        {
            var args = statement.GetArguments();
            var bindVars = new Dictionary<string, object>();
            var placeholders = new object[statement.ArgumentCount];
            for (var i = 0; i < args.Length; ++i)
            {
                var varName = "@var_" + i;
                bindVars[varName] = args[i];
                placeholders[i] = varName;
            }

            return new Query(string.Format(statement.Format, placeholders), bindVars);
        }
    }
}