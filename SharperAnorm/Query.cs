using System;
using System.Collections.Generic;
using System.Linq;

namespace SharperAnorm
{
    /// <summary>
    /// The class represents a database query. Instance will typically be created using one of the two
    /// available factory methods Plain(String) and Parameterized(FormattableString). Instances of this class are
    /// immutable and no interaction with the database is done, it is merely used to tie bind variables to statements,
    /// especially when used with string interpolation for queries with statically known bind variables.
    /// </summary>
    public class Query
    {
        private static string VariablePrefix = "@";
        
        public string Statement { get; }
        public IDictionary<string, object> Parameters { get; }

        private Query(string statement, IDictionary<string, object> parameters)
        {
            Statement = statement;
            Parameters = parameters;
        }

        /// <summary>
        /// Adds a bind variable to this query. The result will be q new Query instance with the new bind variable,
        /// while leaving the this object unchanged.
        /// </summary>
        /// <param name="name">The ame of the bind variable. Depending on the DB driver a leading @ could be necessary.</param>
        /// <param name="value">The value to bind (may be null).</param>
        /// <returns>A new Query instance with the new bind variable.</returns>
        public Query Bind(string name, object value)
        {
            var newParameters = new Dictionary<string, object>(Parameters) {[name] = value};
            return new Query(Statement, newParameters);
        }

        public override string ToString()
        {
            return "[" + Statement + "] with " + string.Join(", ", Parameters.Select(e => e.Key + " = " + e.Value));
        }

        /// <summary>
        /// Wraps a plain SQL statement into a Query instance without any bind variables.
        /// </summary>
        /// <param name="statement">The SQL statement. The statement may contain bind variables, which must be bound later on.</param>
        /// <returns>A Query instance wrapping the given SQL statement.</returns>
        public static Query Plain(string statement)
        {
            return new Query(statement, new Dictionary<string, object>());
        }

        /// <summary>
        /// Generates a parameterized SQL statement based on the given format string and adds bind variables for the applied arguments.
        /// </summary>
        /// <param name="statement">The statement as an interpolated string.</param>
        /// <returns>A Query instance reflecting the given statement and bind values.</returns>
        public static Query Parameterized(FormattableString statement)
        {
            var args = statement.GetArguments();
            var bindVars = new Dictionary<string, object>();
            var placeholders = new object[statement.ArgumentCount];
            for (var i = 0; i < args.Length; ++i)
            {
                var varName = "var_" + i;
                bindVars[varName] = args[i];
                placeholders[i] = VariablePrefix + varName;
            }

            return new Query(string.Format(statement.Format, placeholders), bindVars);
        }
    }
}