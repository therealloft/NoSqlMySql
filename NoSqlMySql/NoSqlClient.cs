using MySql.Data.MySqlClient;
using System.Collections.Generic;

namespace NoSqlMySql
{
    public class NoSqlClient
    {
        internal string ConnectionString { get; private set; }
        public NoSqlClient(string datasource, int port, string username, string password)
        {
            ConnectionString = $"datasource={datasource};port={port};username={username};password={password}";
        }
        public NoSqlClient(string connectionString)
        {
            ConnectionString = connectionString;
        }
        public bool CreateDatabase(string database)
        {
            bool success = false;
            MySqlConnection connection = new MySqlConnection(ConnectionString);
            MySqlCommand command = new MySqlCommand($"CREATE DATABASE IF NOT EXISTS {database};", connection);
            connection.Open();
            success = command.ExecuteNonQuery() > 1;
            connection.Close();
            return success;
        }
        public bool DropDatabase(string database)
        {
            bool success = false;
            if (database.ToLower() == "information_schema") return success;
            MySqlConnection connection = new MySqlConnection(ConnectionString);
            MySqlCommand command = new MySqlCommand($"DROP DATABASE {database};", connection);
            connection.Open();
            success = command.ExecuteNonQuery() > 1;
            connection.Close();
            return success;
        }
        public List<string> GetDatabaseNames()
        {
            List<string> names = new List<string>();
            MySqlConnection connection = new MySqlConnection(ConnectionString);
            MySqlCommand command = new MySqlCommand($"SHOW DATABASE;", connection);
            connection.Open();
            MySqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                string name = reader.GetString("Database");
                if (name != "information_scheme")
                    names.Add(name);
            }
            reader.Close();
            connection.Close();
            return names;
        }
        public NoSqlDatabase GetDatabase(string database)
        {
            return new NoSqlDatabase(this, database);
        }
    }
}
