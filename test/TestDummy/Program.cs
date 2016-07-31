using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace TestDummy
{
    class Program
    {
        const int Seconds = 10;
        static readonly double Milliseconds = TimeSpan.FromSeconds(Seconds).TotalMilliseconds;

        static void Main(string[] args)
        {
            var conn = new NpgsqlConnection("Server=localhost;User ID=npgsql_tests;Password=npgsql_tests;Database=npgsql_tests");
            var cmd = new NpgsqlCommand("SET lock_timeout = 1000", conn);
            //conn.Open();

            var ops = 0;
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < Milliseconds)
            {
                conn.Open();
                cmd.ExecuteNonQuery();
                conn.Close();
                ops++;
            }

            Console.WriteLine($"op/s {ops/Seconds}");
        }
    }
}
