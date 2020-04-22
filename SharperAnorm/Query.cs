using System;
using System.Collections.Generic;

namespace SharperAnorm
{
    public class Query
    {
        public string Statement { get; }
        public IDictionary<string, object> Parameters { get; }

        public Query(string statement, IDictionary<string, object> parameters)
        {
            Statement = statement;
            Parameters = parameters;
        }

        public Query Bind(string name, object value)
        {
            var newParameters = new Dictionary<string, object>(Parameters) {[name] = value};
            return new Query(Statement, newParameters);
        }

        public static Query Sql(string statement)
        {
            return new Query(statement, new Dictionary<string, object>());
        }

        public static Query Sql(FormattableString statement)
        {
            throw new Exception("TODO");
        }
    }
}