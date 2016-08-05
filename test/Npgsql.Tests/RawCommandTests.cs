using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Npgsql.Tests
{
    public class RawCommandTests : TestBase
    {
        [Test]
        public void NoParameters()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlRawCommand("SELECT 8", conn))
            using (var reader = cmd.ExecuteReader())
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetInt32(0), Is.EqualTo(8));
                Assert.That(reader.NextResult(), Is.False);
            }
        }

        [Test]
        public void SingleStatement()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlRawCommand("SELECT $1", conn)
            {
                Parameters = { new NpgsqlParameter { Value = 8 } }
            })
            using (var reader = cmd.ExecuteReader())
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetInt32(0), Is.EqualTo(8));
                Assert.That(reader.NextResult(), Is.False);
            }
        }

        [Test]
        public void MultipleStatements()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlRawCommand(conn)
            {
                Statements =
                {
                    new NpgsqlStatement
                    {
                        SQL = "SELECT $1",
                        InputParameters = { new NpgsqlParameter { Value = 8 } }
                    },
                    new NpgsqlStatement
                    {
                        SQL = "SELECT $1",
                        InputParameters = { new NpgsqlParameter { Value = 9 } }
                    }
                }
            })
            using (var reader = cmd.ExecuteReader())
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetInt32(0), Is.EqualTo(8));
                Assert.That(reader.NextResult(), Is.True);
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader.GetInt32(0), Is.EqualTo(9));
                Assert.That(reader.NextResult(), Is.False);
            }
        }

        [Test]
        public void NoOutputParameters()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlRawCommand("SELECT 1", conn))
            {
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p", Direction = ParameterDirection.Output });
                Assert.That(() => cmd.ExecuteScalar(), Throws.Exception.TypeOf<InvalidOperationException>());
                cmd.Parameters.Clear();
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p", Direction = ParameterDirection.InputOutput });
                Assert.That(() => cmd.ExecuteScalar(), Throws.Exception.TypeOf<InvalidOperationException>());
            }
        }

        [Test]
        public void CommandText()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlRawCommand(conn)
            {
                Statements =
                {
                    new NpgsqlStatement { SQL = "SELECT 1" },
                    new NpgsqlStatement { SQL = "SELECT 2" }
                }
            })
            {
                Assert.That(cmd.CommandText, Is.EqualTo("SELECT 1; SELECT 2"));
                Assert.That(() => { cmd.CommandText = "boom"; }, Throws.Exception.TypeOf<NotSupportedException>());
            }
        }

        [Test]
        public void CommandType()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlRawCommand("SELECT 1", conn))
            {
                Assert.That(cmd.CommandType, Is.EqualTo(System.Data.CommandType.Text));
                Assert.That(() => { cmd.CommandType = System.Data.CommandType.StoredProcedure; }, Throws.Exception.TypeOf<NotSupportedException>());
            }
        }
    }
}
