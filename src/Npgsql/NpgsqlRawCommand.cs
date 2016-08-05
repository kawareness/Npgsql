using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Npgsql
{
    /// <summary>
    /// Represents a SQL statement or function (stored procedure) to execute
    /// against a PostgreSQL database. Unlike <see cref="NpgsqlCommand"/>, this class does not perform
    /// any SQL parsing or substitution, and does not use <see cref="NpgsqlCommand.CommandText"/>.
    /// This class cannot be inherited.
    /// </summary>
#if NETSTANDARD1_3
    public sealed class NpgsqlRawCommand : NpgsqlCommandBase
#else
    public sealed class NpgsqlRawCommand : NpgsqlCommandBase, ICloneable
#endif

    {
        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlRawCommand"/> class.
        /// </summary>
        public NpgsqlRawCommand() {}

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlRawCommand"/> class.
        /// <param name="connection">A <see cref="NpgsqlConnection"/> that represents the connection to a PostgreSQL server.</param>
        /// </summary>
        public NpgsqlRawCommand(NpgsqlConnection connection)
        {
            Connection = connection;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlRawCommand"/> class.
        /// <param name="connection">A <see cref="NpgsqlConnection"/> that represents the connection to a PostgreSQL server.</param>
        /// <param name="transaction">The <see cref="NpgsqlTransaction">NpgsqlTransaction</see> in which the <see cref="NpgsqlRawCommand">NpgsqlRawCommand</see> executes.</param>
        /// </summary>
        public NpgsqlRawCommand(NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            Connection = connection;
            Transaction = transaction;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlRawCommand"/> class with the text of the query.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        // ReSharper disable once IntroduceOptionalParameters.Global
        public NpgsqlRawCommand(string cmdText)
        {
            Statements.Add(new NpgsqlStatement { SQL = cmdText });
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlRawCommand"/> class with the text of the query and a <see cref="NpgsqlConnection">NpgsqlConnection</see>.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        /// <param name="connection">A <see cref="NpgsqlConnection"/> that represents the connection to a PostgreSQL server.</param>
        // ReSharper disable once IntroduceOptionalParameters.Global
        public NpgsqlRawCommand(string cmdText, NpgsqlConnection connection) : this(cmdText)
        {
            Connection = connection;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlRawCommand">NpgsqlRawCommand</see> class with the text of the query, a <see cref="NpgsqlConnection">NpgsqlConnection</see>, and the <see cref="NpgsqlTransaction">NpgsqlTransaction</see>.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        /// <param name="connection">A <see cref="NpgsqlConnection">NpgsqlConnection</see> that represents the connection to a PostgreSQL server.</param>
        /// <param name="transaction">The <see cref="NpgsqlTransaction">NpgsqlTransaction</see> in which the <see cref="NpgsqlRawCommand">NpgsqlRawCommand</see> executes.</param>
        public NpgsqlRawCommand(string cmdText, NpgsqlConnection connection, NpgsqlTransaction transaction)
            : this(cmdText, connection)
        {
            Transaction = transaction;
        }

        #endregion Constructors

        #region Public properties

        /// <summary>
        /// Contains the list of individual statements that make up this command.
        /// </summary>
        public IList<NpgsqlStatement> Statements => _statements;

        /// <summary>
        /// Not supported in NpgsqlRawCommand, use <see cref="Statements"/> instead.
        /// </summary>
        [Category("Data")]
        public override string CommandText
        {
            get
            {
                switch (Statements.Count)
                {
                case 0:
                    return string.Empty;
                case 1:
                    return Statements[0].SQL;
                default:
                    return string.Join("; ", Statements);
                }
            }
            set { throw new NotSupportedException($"CommandText isn't supported on {nameof(NpgsqlRawCommand)}, use {Statements} instead"); }
        }

        /// <summary>
        /// Not supported in NpgsqlRawCommand.
        /// </summary>
        [Category("Data")]
        public override CommandType CommandType
        {
            get { return CommandType.Text; }
            set { throw new NotSupportedException($"CommandType isn't supported on {nameof(NpgsqlRawCommand)}, use {Statements} instead"); }
        }

        #endregion

        #region Execute

        /// <summary>
        /// Execution logic specific to <see cref="NpgsqlRawCommand"/>.
        /// </summary>
        protected override NpgsqlDataReader Execute(CommandBehavior behavior = CommandBehavior.Default)
        {
            ProcessParameters();
            return base.Execute(behavior);
        }

        /// <summary>
        /// Execution logic specific to <see cref="NpgsqlCommand"/>.
        /// Parses <see cref="CommandText"/> and constructs the <see cref="Statements"/> list from it.
        /// </summary>
        protected override Task<NpgsqlDataReader> ExecuteAsync(CancellationToken cancellationToken, CommandBehavior behavior = CommandBehavior.Default)
        {
            ProcessParameters();
            return base.ExecuteAsync(cancellationToken, behavior);
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
            ProcessParameters();
            base.Prepare(persist);
        }

        void ProcessParameters()
        {
            if (Parameters.Count > 0)
            {
                // With NpgsqlRawCommand, parameters should be set on the individual statements and not on the
                // command. However, if there's a single statement we allow parameters to be set on the command
                // for a nicer API.
                if (Statements.Count != 1)
                    throw new InvalidOperationException("Add parameters to your individual statements, not to the command");
                Statements[0].InputParameters.Clear();
                Statements[0].InputParameters.AddRange(Parameters);
            }

            // Only input parameters are supported
            if (Statements.SelectMany(s => s.InputParameters).Any(p => p.IsOutputDirection))
                throw new InvalidOperationException($"{nameof(NpgsqlRawCommand)} only accepts input parameters");
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
        public NpgsqlRawCommand Clone()
        {
            var clone = new NpgsqlRawCommand(Connection, Transaction)
            {
                CommandTimeout = CommandTimeout,
                CommandType = CommandType,
                DesignTimeVisible = DesignTimeVisible,
                AllResultTypesAreUnknown = AllResultTypesAreUnknown,
                UnknownResultTypeList = UnknownResultTypeList,
                ObjectResultTypes = ObjectResultTypes
            };
            throw new NotImplementedException("Clone statements");
            //Parameters.CloneTo(clone.Parameters);
            //return clone;
        }

        #endregion
    }
}
