using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
#if NET46
using BenchmarkDotNet.Diagnostics.Windows;
#endif

namespace Npgsql.Benchmarks
{
    [Config(typeof(Config))]
    public class RawCommand
    {
        NpgsqlCommand _simpleCmd;
        NpgsqlRawCommand _simpleRawCmd;
        NpgsqlCommand _withParamCmd;
        NpgsqlRawCommand _withParamRawCmd;
        NpgsqlCommand _bigMultistatementCmd;
        NpgsqlRawCommand _bigMultistatementRawCmd;

        [Setup]
        public void Setup()
        {
            var conn = BenchmarkEnvironment.OpenConnection();

            var query = $"SELECT '{new string('x', 1000)}'";
            _simpleCmd = new NpgsqlCommand(query, conn);
            _simpleRawCmd = new NpgsqlRawCommand(query, conn);

            _withParamCmd = new NpgsqlCommand("SELECT @p", conn);
            _withParamCmd.Parameters.AddWithValue("p", 8);
            _withParamRawCmd = new NpgsqlRawCommand("SELECT $1", conn);
            _withParamRawCmd.Parameters.Add(new NpgsqlParameter { Value = 8 });

            _bigMultistatementCmd = new NpgsqlCommand { Connection = conn };
            _bigMultistatementRawCmd = new NpgsqlRawCommand(conn);
            var sb = new StringBuilder();
            for (var i = 1; i <= 1000; i++)
            {
                sb.Append($"SELECT @p{i};");
                _bigMultistatementCmd.Parameters.Add(new NpgsqlParameter($"p{i}", 8));
                _bigMultistatementRawCmd.Statements.Add(new NpgsqlStatement("SELECT $1")
                {
                    InputParameters = { new NpgsqlParameter { Value = 8 } }
                });
            }
            _bigMultistatementCmd.CommandText = sb.ToString();
        }

        [Benchmark]
        public void NpgsqlCommand() => _simpleCmd.ExecuteNonQuery();

        [Benchmark]
        public void NpgsqlRawCommand() => _simpleRawCmd.ExecuteNonQuery();

        [Benchmark]
        public int WithParam() => (int)_withParamCmd.ExecuteScalar();

        [Benchmark]
        public int WithParamRaw() => (int)_withParamRawCmd.ExecuteScalar();

        [Benchmark]
        public int BigMultistatement()
        {
            var sum = 0;
            using (var reader = _bigMultistatementCmd.ExecuteReader())
            {
                while (reader.Read())
                    sum += reader.GetInt32(0);
            }
            return sum;
        }

        [Benchmark]
        public int BigMultistatementRaw()
        {
            var sum = 0;
            using (var reader = _bigMultistatementRawCmd.ExecuteReader())
            {
                while (reader.Read())
                    sum += reader.GetInt32(0);
            }
            return sum;
        }

        class Config : ManualConfig
        {
            public Config()
            {
                Add(new Job { TargetCount = 20 });
#if NET46
                Add(new MemoryDiagnoser());
#endif
                Add(StatisticColumn.OperationsPerSecond);
            }
        }
    }
}
