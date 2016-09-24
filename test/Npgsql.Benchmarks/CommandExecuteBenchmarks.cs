﻿using System;
using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
#if NET46
using BenchmarkDotNet.Diagnostics.Windows;
#endif
// ReSharper disable UnusedMember.Global

namespace Npgsql.Benchmarks
{
    [SuppressMessage("ReSharper", "AssignNullToNotNullAttribute")]
    [Config(typeof(Config))]
    public class CommandExecuteBenchmarks
    {
        readonly NpgsqlCommand _executeNonQueryCmd;
        readonly NpgsqlCommand _executeNonQueryWithParamCmd;
        readonly NpgsqlCommand _executeNonQueryPreparedCmd;
        readonly NpgsqlRawCommand _executeNonQueryRawCmd;
        readonly NpgsqlCommand _executeScalarCmd;
        readonly NpgsqlCommand _executeReaderCmd;

        public CommandExecuteBenchmarks()
        {
            var conn = BenchmarkEnvironment.OpenConnection();
            _executeNonQueryCmd = new NpgsqlCommand("SET lock_timeout = 1000", conn);
            _executeNonQueryWithParamCmd = new NpgsqlCommand("SET lock_timeout = 1000", conn);
            _executeNonQueryWithParamCmd.Parameters.AddWithValue("not_used", DBNull.Value);
            _executeNonQueryPreparedCmd = new NpgsqlCommand("SET lock_timeout = 1000", conn);
            _executeNonQueryPreparedCmd.Prepare();
            _executeNonQueryRawCmd = new NpgsqlRawCommand("SET lock_timeout = 1000", conn);
            _executeScalarCmd = new NpgsqlCommand("SELECT 1", conn);
            _executeReaderCmd   = new NpgsqlCommand("SELECT 1", conn);
        }

        [Benchmark]
        public int ExecuteNonQuery() => _executeNonQueryCmd.ExecuteNonQuery();

        [Benchmark]
        public int ExecuteNonQueryWithParam() => _executeNonQueryWithParamCmd.ExecuteNonQuery();

        [Benchmark]
        public int ExecuteNonQueryPrepared() => _executeNonQueryPreparedCmd.ExecuteNonQuery();

        [Benchmark]
        public int ExecuteNonQueryRaw() => _executeNonQueryRawCmd.ExecuteNonQuery();

        [Benchmark]
        public object ExecuteScalar() => _executeScalarCmd.ExecuteScalar();

        [Benchmark]
        public object ExecuteReader()
        {
            using (var reader = _executeReaderCmd.ExecuteReader())
            {
                reader.Read();
                return reader.GetValue(0);
            }
        }

        class Config : ManualConfig
        {
            public Config()
            {
#if NET46
                Add(new MemoryDiagnoser());
#endif
                Add(StatisticColumn.OperationsPerSecond);
            }
        }
    }
}
