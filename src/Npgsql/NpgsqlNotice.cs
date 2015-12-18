﻿#region License
// The PostgreSQL License
//
// Copyright (C) 2015 The Npgsql Development Team
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement is hereby granted, provided that the above copyright notice
// and this paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE NPGSQL DEVELOPMENT TEAM BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
// INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE NPGSQL DEVELOPMENT TEAM HAS BEEN ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//
// THE NPGSQL DEVELOPMENT TEAM SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE NPGSQL DEVELOPMENT TEAM HAS NO OBLIGATIONS
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#endregion

using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using Npgsql.BackendMessages;

namespace Npgsql
{
    /// <summary>
    /// A non-critical (warning or info) message generated by the backend.
    /// Can be synchronous (i.e. in response to a query) or asynchronous (a totally unrelated
    /// backend-side event).
    /// </summary>
    /// <remarks>
    /// http://www.postgresql.org/docs/current/static/protocol-flow.html#PROTOCOL-ASYNC
    /// </remarks>
    public class NpgsqlNotice
    {
        readonly ErrorOrNoticeMessage _msg;

        #region Message Fields

        /// <summary>
        /// Severity of the error or notice.
        /// Always present.
        /// </summary>
        public string Severity => _msg.Severity;

        /// <summary>
        /// The SQLSTATE code for the error.
        /// </summary>
        /// <remarks>
        /// Always present.
        /// See http://www.postgresql.org/docs/current/static/errcodes-appendix.html
        /// </remarks>
        public string Code => _msg.Code;

        /// <summary>
        /// The primary human-readable error message. This should be accurate but terse.
        /// </summary>
        /// <remarks>
        /// Always present.
        /// </remarks>
        public string MessageText => _msg.Message;

        /// <summary>
        /// An optional secondary error message carrying more detail about the problem.
        /// May run to multiple lines.
        /// </summary>
        public string Detail => _msg.Detail;

        /// <summary>
        /// An optional suggestion what to do about the problem.
        /// This is intended to differ from Detail in that it offers advice (potentially inappropriate) rather than hard facts.
        /// May run to multiple lines.
        /// </summary>
        public string Hint => _msg.Hint;

        /// <summary>
        /// The field value is a decimal ASCII integer, indicating an error cursor position as an index into the original query string.
        /// The first character has index 1, and positions are measured in characters not bytes.
        /// 0 means not provided.
        /// </summary>
        public int Position => _msg.Position;

        /// <summary>
        /// This is defined the same as the <see cref="Position"/> field, but it is used when the cursor position refers to an internally generated command rather than the one submitted by the client.
        /// The <see cref="InternalQuery" /> field will always appear when this field appears.
        /// 0 means not provided.
        /// </summary>
        public int InternalPosition => _msg.InternalPosition;

        /// <summary>
        /// The text of a failed internally-generated command.
        /// This could be, for example, a SQL query issued by a PL/pgSQL function.
        /// </summary>
        public string InternalQuery => _msg.InternalQuery;

        /// <summary>
        /// An indication of the context in which the error occurred.
        /// Presently this includes a call stack traceback of active PL functions.
        /// The trace is one entry per line, most recent first.
        /// </summary>
        public string Where => _msg.Where;

        /// <summary>
        /// If the error was associated with a specific database object, the name of the schema containing that object, if any.
        /// </summary>
        /// <remarks>PostgreSQL 9.3 and up.</remarks>
        public string SchemaName => _msg.SchemaName;

        /// <summary>
        /// Table name: if the error was associated with a specific table, the name of the table.
        /// (Refer to the schema name field for the name of the table's schema.)
        /// </summary>
        /// <remarks>PostgreSQL 9.3 and up.</remarks>
        public string TableName => _msg.TableName;

        /// <summary>
        /// If the error was associated with a specific table column, the name of the column.
        /// (Refer to the schema and table name fields to identify the table.)
        /// </summary>
        /// <remarks>PostgreSQL 9.3 and up.</remarks>
        public string ColumnName => _msg.ColumnName;

        /// <summary>
        /// If the error was associated with a specific data type, the name of the data type.
        /// (Refer to the schema name field for the name of the data type's schema.)
        /// </summary>
        /// <remarks>PostgreSQL 9.3 and up.</remarks>
        public string DataTypeName => _msg.DataTypeName;

        /// <summary>
        /// If the error was associated with a specific constraint, the name of the constraint.
        /// Refer to fields listed above for the associated table or domain.
        /// (For this purpose, indexes are treated as constraints, even if they weren't created with constraint syntax.)
        /// </summary>
        /// <remarks>PostgreSQL 9.3 and up.</remarks>
        public string ConstraintName => _msg.ConstraintName;

        /// <summary>
        /// The file name of the source-code location where the error was reported.
        /// </summary>
        /// <remarks>PostgreSQL 9.3 and up.</remarks>
        public string File => _msg.File;

        /// <summary>
        /// The line number of the source-code location where the error was reported.
        /// </summary>
        public string Line => _msg.Line;

        /// <summary>
        /// The name of the source-code routine reporting the error.
        /// </summary>
        public string Routine => _msg.Routine;

        #endregion

        internal NpgsqlNotice(ReadBuffer buf)
        {
            _msg = new ErrorOrNoticeMessage(buf);
        }
    }

    /// <summary>
    /// Provides data for a notice event.
    /// </summary>
    public class NpgsqlNoticeEventArgs : EventArgs
    {
        /// <summary>
        /// The Notice that was sent from the database.
        /// </summary>
        public NpgsqlNotice Notice { get; private set; }

        internal NpgsqlNoticeEventArgs(NpgsqlNotice notice)
        {
            Notice = notice;
        }
    }
}
