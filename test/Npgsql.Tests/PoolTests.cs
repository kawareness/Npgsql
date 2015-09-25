#region License
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
using System.Linq;
using System.Text;
using System.Threading;
using NUnit.Framework;

namespace Npgsql.Tests
{
    class PoolTests : TestBase
    {
        [Test]
        public void MinPoolSizeEqualsMaxPoolSize()
        {
            using (var conn = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(ConnectionString) {
                MinPoolSize = 30,
                MaxPoolSize = 30
            }))
            {
                conn.Open();
            }
        }

        [Test]
        public void MinPoolSizeLargeThanMaxPoolSize()
        {
            using (var conn = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(ConnectionString)
            {
                MinPoolSize = 2,
                MaxPoolSize = 1
            }))
            {
                Assert.That(() => conn.Open(), Throws.Exception.TypeOf<ArgumentException>());
            }
        }

        [Test]
        public void MinPoolSizeLargeThanPoolSizeLimit()
        {
            var csb = new NpgsqlConnectionStringBuilder(ConnectionString);
            Assert.That(() => csb.MinPoolSize = PoolManager.PoolSizeLimit + 1, Throws.Exception.TypeOf<ArgumentOutOfRangeException>());
        }

        [Test]
        public void MinPoolSize()
        {
            var connString = new NpgsqlConnectionStringBuilder(ConnectionString) { MinPoolSize = 2 };
            using (var conn = new NpgsqlConnection(connString))
            {
                connString = conn.Settings; // Shouldn't be necessary
                conn.Open();
                conn.Close();
            }

            var pool = PoolManager.Pools[connString];
            Assert.That(pool.Idle, Has.Count.EqualTo(2));
            /*
            using (var conn1 = new NpgsqlConnection(connString))
            using (var conn2 = new NpgsqlConnection(connString))
            using (var conn3 = new NpgsqlConnection(connString))
            {
                conn1.Open(); conn2.Open(); conn3.Open();
            }
            Assert.That(pool.Idle, Has.Count.EqualTo(2));*/
        }

        [Test]
        public void ReuseConnectorBeforeCreatingNew()
        {
            using (var conn = new NpgsqlConnection(ConnectionString))
            {
                conn.Open();
                var backendId = conn.Connector.BackendProcessId;
                conn.Close();
                conn.Open();
                Assert.That(conn.Connector.BackendProcessId, Is.EqualTo(backendId));
            }
        }

        [Test]
        public void GetConnectorFromExhaustedPool()
        {
            var connString = new NpgsqlConnectionStringBuilder(ConnectionString) {
                MaxPoolSize = 1,
                Timeout = 0
            };

            using (var conn1 = new NpgsqlConnection(connString))
            {
                conn1.Open();
                // Pool is exhausted
                using (var conn2 = new NpgsqlConnection(connString))
                {
                    new Timer(o => conn1.Close(), null, 1000, Timeout.Infinite);
                    conn2.Open();
                }
            }
        }

        [Test]
        public void TimeoutGettingConnectorFromExhaustedPool()
        {
            var connString = new NpgsqlConnectionStringBuilder(ConnectionString) {
                MaxPoolSize = 1,
                Timeout = 1
            };

            using (var conn1 = new NpgsqlConnection(connString))
            {
                conn1.Open();
                // Pool is exhausted
                using (var conn2 = new NpgsqlConnection(connString))
                {
                    Assert.That(() => conn2.Open(), Throws.Exception.TypeOf<TimeoutException>());
                }
            }
            // conn1 should now be back in the pool as idle
            using (var conn3 = new NpgsqlConnection(connString))
            {
                conn3.Open();
            }
        }

        [Test, Description("Makes sure that when a pooled connection is closed it's properly reset, and that parameter settings aren't leaked")]
        public void ResetOnClose()
        {
            using (var conn = new NpgsqlConnection(ConnectionString + ";SearchPath=public"))
            {
                conn.Open();
                Assert.That(conn.ExecuteScalar("SHOW search_path"), Is.Not.Contains("pg_temp"));
                var backendId = conn.Connector.BackendProcessId;
                conn.ExecuteNonQuery("SET search_path=pg_temp");
                conn.Close();

                conn.Open();
                Assert.That(conn.Connector.BackendProcessId, Is.EqualTo(backendId));
                Assert.That(conn.ExecuteScalar("SHOW search_path"), Is.EqualTo("public"));
            }
        }

        public PoolTests(string backendVersion) : base(backendVersion) {}
    }
}
