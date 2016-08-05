using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Npgsql
{
    /// <summary>
    /// Represents a SQL statement or function (stored procedure) to execute against a PostgreSQL database.
    /// </summary>
#if NETSTANDARD1_3
    public sealed class NpgsqlCommand : NpgsqlCommandBase
#else
    public sealed class NpgsqlCommand : NpgsqlCommandBase, ICloneable
#endif
    {
        string _commandText;

        /// <summary>
        /// Returns details about each statement that this command has executed.
        /// Is only populated when an Execute* method is called.
        /// </summary>
        public IReadOnlyList<NpgsqlStatement> Statements => _statements.AsReadOnly();

        readonly SqlQueryParser _sqlParser = new SqlQueryParser();

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlCommand">NpgsqlCommand</see> class.
        /// </summary>
        public NpgsqlCommand() : this(string.Empty, null, null) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlCommand">NpgsqlCommand</see> class with the text of the query.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        // ReSharper disable once IntroduceOptionalParameters.Global
        public NpgsqlCommand(string cmdText) : this(cmdText, null, null) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlCommand">NpgsqlCommand</see> class with the text of the query and a <see cref="NpgsqlConnection">NpgsqlConnection</see>.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        /// <param name="connection">A <see cref="NpgsqlConnection">NpgsqlConnection</see> that represents the connection to a PostgreSQL server.</param>
        // ReSharper disable once IntroduceOptionalParameters.Global
        public NpgsqlCommand(string cmdText, NpgsqlConnection connection) : this(cmdText, connection, null) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlCommand">NpgsqlCommand</see> class with the text of the query, a <see cref="NpgsqlConnection">NpgsqlConnection</see>, and the <see cref="NpgsqlTransaction">NpgsqlTransaction</see>.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        /// <param name="connection">A <see cref="NpgsqlConnection">NpgsqlConnection</see> that represents the connection to a PostgreSQL server.</param>
        /// <param name="transaction">The <see cref="NpgsqlTransaction">NpgsqlTransaction</see> in which the <see cref="NpgsqlCommand">NpgsqlCommand</see> executes.</param>
        public NpgsqlCommand(string cmdText, [CanBeNull] NpgsqlConnection connection, [CanBeNull] NpgsqlTransaction transaction)
        {
            _commandText = cmdText;
            CommandType = CommandType.Text;
            Connection = connection;
            Transaction = transaction;
        }

        #endregion

        #region Query analysis

        void ProcessCommandText()
        {
            NpgsqlStatement statement;
            switch (CommandType)
            {
            case CommandType.Text:
                _sqlParser.ParseRawQuery(CommandText, Connection == null || Connection.UseConformantStrings, Parameters, _statements);
                if (_statements.Count > 1 && Parameters.Any(p => p.IsOutputDirection))
                    throw new NotSupportedException("Commands with multiple queries cannot have out parameters");
                break;

            case CommandType.TableDirect:
                if (_statements.Count == 0)
                    statement = new NpgsqlStatement();
                else
                {
                    statement = _statements[0];
                    statement.Reset();
                    _statements.Clear();
                }
                _statements.Add(statement);
                statement.SQL = "SELECT * FROM " + CommandText;
                break;

            case CommandType.StoredProcedure:
                var inputList = Parameters.Where(p => p.IsInputDirection).ToList();
                var numInput = inputList.Count;
                var sb = new StringBuilder();
                sb.Append("SELECT * FROM ");
                sb.Append(CommandText);
                sb.Append('(');
                bool hasWrittenFirst = false;
                for (var i = 1; i <= numInput; i++)
                {
                    var param = inputList[i - 1];
                    if (param.AutoAssignedName || param.CleanName == "")
                    {
                        if (hasWrittenFirst)
                        {
                            sb.Append(',');
                        }
                        sb.Append('$');
                        sb.Append(i);
                        hasWrittenFirst = true;
                    }
                }
                for (var i = 1; i <= numInput; i++)
                {
                    var param = inputList[i - 1];
                    if (!param.AutoAssignedName && param.CleanName != "")
                    {
                        if (hasWrittenFirst)
                        {
                            sb.Append(',');
                        }
                        sb.Append('"');
                        sb.Append(param.CleanName.Replace("\"", "\"\""));
                        sb.Append("\" := ");
                        sb.Append('$');
                        sb.Append(i);
                        hasWrittenFirst = true;
                    }
                }
                sb.Append(')');

                if (_statements.Count == 0)
                    statement = new NpgsqlStatement();
                else
                {
                    statement = _statements[0];
                    statement.Reset();
                    _statements.Clear();
                }
                statement.SQL = sb.ToString();
                statement.InputParameters.AddRange(inputList);
                _statements.Add(statement);
                break;
            default:
                throw new InvalidOperationException($"Internal Npgsql bug: unexpected value {CommandType} of enum {nameof(CommandType)}. Please file a bug.");
            }
        }

        #endregion

        #region Public properties

        /// <summary>
        /// Gets or sets the SQL statement or function (stored procedure) to execute at the data source.
        /// </summary>
        /// <value>The Transact-SQL statement or stored procedure to execute. The default is an empty string.</value>
        [DefaultValue("")]
        [Category("Data")]
        public override string CommandText
        {
            get { return _commandText; }
            set
            {
                if (value == null)
                    throw new ArgumentNullException(nameof(value));

                _commandText = value;
                Unprepare();
            }
        }

        /// <summary>
        /// Gets or sets a value indicating how the
        /// <see cref="CommandText">CommandText</see> property is to be interpreted.
        /// </summary>
        /// <value>One of the <see cref="System.Data.CommandType">CommandType</see> values. The default is <see cref="System.Data.CommandType">CommandType.Text</see>.</value>
        [DefaultValue(CommandType.Text)]
        [Category("Data")]
        public override CommandType CommandType { get; set; }

        #endregion

        #region Execute

        /// <summary>
        /// Execution logic specific to <see cref="NpgsqlCommand"/>.
        /// Parses <see cref="CommandText"/> and constructs the <see cref="Statements"/> list from it.
        /// </summary>
        protected override NpgsqlDataReader Execute(CommandBehavior behavior = CommandBehavior.Default)
        {
            if (!IsPrepared)
                ProcessCommandText();
            return base.Execute(behavior);
        }

        /// <summary>
        /// Execution logic specific to <see cref="NpgsqlCommand"/>.
        /// Parses <see cref="CommandText"/> and constructs the <see cref="Statements"/> list from it.
        /// </summary>
        protected override Task<NpgsqlDataReader> ExecuteAsync(CancellationToken cancellationToken, CommandBehavior behavior = CommandBehavior.Default)
        {
            if (!IsPrepared)
                ProcessCommandText();
            return base.ExecuteAsync(cancellationToken, behavior);
        }

        /// <summary>
        /// Executes the command using the PostgreSQL simple protocol, which is better for performance
        /// reasons. Only relevant for unprepared unparameterized non-queries.
        /// </summary>
        protected override int ExecuteSimple()
        {
            ProcessCommandText();
            return base.ExecuteSimple();
        }

        /// <summary>
        /// Creates a prepared version of the command on a PostgreSQL server.
        /// </summary>
        /// <param name="persist">
        /// If set to true, prepared statements are persisted when a pooled connection is closed for later use.
        /// </param>
        public override void Prepare(bool persist)
        {
            Unprepare();
            ProcessCommandText();
            base.Prepare(persist);
        }

        #endregion

        #region Misc

#if NET45 || NET451
        /// <summary>
        /// Create a new command based on this one.
        /// </summary>
        /// <returns>A new NpgsqlCommand object.</returns>
        object ICloneable.Clone() => Clone();
#endif

        /// <summary>
        /// Create a new command based on this one.
        /// </summary>
        /// <returns>A new NpgsqlCommand object.</returns>
        [PublicAPI]
        public NpgsqlCommand Clone()
        {
            var clone = new NpgsqlCommand(CommandText, Connection, Transaction)
            {
                CommandTimeout = CommandTimeout,
                CommandType = CommandType,
                DesignTimeVisible = DesignTimeVisible,
                AllResultTypesAreUnknown = AllResultTypesAreUnknown,
                UnknownResultTypeList = UnknownResultTypeList,
                ObjectResultTypes = ObjectResultTypes
            };
            Parameters.CloneTo(clone.Parameters);
            return clone;
        }

        #endregion
    }
}
