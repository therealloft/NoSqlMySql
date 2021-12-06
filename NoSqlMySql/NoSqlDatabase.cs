using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NoSqlMySql
{
    public class NoSqlDatabase
    {
        private NoSqlClient client;
        private string schema;
        public NoSqlDatabase(NoSqlClient client, string schema)
        {
            this.client = client;
            this.schema = schema;
        }
        public void CreateIfNotExists(string database)
        {
            string sql = $"CREATE TABLE IF NOT EXISTS `{schema}`.`{database}` (`id` VARCHAR(36) NOT NULL, `data` LONGTEXT NOT NULL, PRIMARY KEY(`id`)) ENGINE = InnoDB;";
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand command = new MySqlCommand(sql, connection);
            connection.Open();
            command.ExecuteNonQuery();
            connection.Close();
        }
        public void CreateCollection(string database)
        {
            string sql = $"CREATE DATABASE IF NOT EXISTS {database}";
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand command = new MySqlCommand(sql, connection);
            connection.Open();
            command.ExecuteNonQuery();
            connection.Close();
        }
        public void DropCollection(string database)
        {
            string sql = $"DROP DATABASE {database}";
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand command = new MySqlCommand(sql, connection);
            connection.Open();
            command.ExecuteNonQuery();
            connection.Close();
        }
        public NoSqlCollection GetCollection(string database)
        {
            CreateIfNotExists(database);
            return new NoSqlCollection(client, schema, database);
        }
        public List<string> ListCollections()
        {
            List<string> names = new List<string>();
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand command = new MySqlCommand($"SHOW TABLES FROM {schema};", connection);
            connection.Open();
            MySqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                names.Add(reader.GetString($"Tables_in_{schema}"));
            }
            reader.Close();
            connection.Close();
            return names;
        }
    }
}
