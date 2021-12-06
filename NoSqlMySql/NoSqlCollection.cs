using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NoSqlMySql
{
    public class NoSqlCollection
    {
        private NoSqlClient client;
        private string schema;
        private string table;
        public NoSqlCollection(NoSqlClient client, string schema, string table)
        {
            this.client = client;
            this.schema = schema;
            this.table = table;
        }
        public int Count()
        {
            int count = 0;
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand command = new MySqlCommand($"SELECT Count(*) AS Total FROM {schema}.{table}", connection);
            connection.Open();
            MySqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                count = reader.GetInt32("Total");
            }
            reader.Close();
            connection.Close();
            return count;
        }
        public void DeleteMany(Guid[] ids)
        {
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            connection.Open();
            foreach(Guid id in ids)
            {
                string sql = $"DELETE FROM {schema}.{table} WHERE id='{id.ToString()}'";
                MySqlCommand command = new MySqlCommand(sql, connection);
                command.ExecuteNonQuery();
            }
            connection.Close();
        }
        public void DeleteOne(Guid id)
        {
            string sql = $"DELETE FROM {schema}.{table} WHERE id='{id.ToString()}'";
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand command = new MySqlCommand(sql, connection);
            connection.Open();
            command.ExecuteNonQuery();
            connection.Close();
        }
        public T GetOne<T>(Guid id)
        {
            T result = default(T);
            string sql = $"SELECT * FROM {schema}.{table} WHERE id='{id.ToString()}'";
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand command = new MySqlCommand(sql, connection);
            connection.Open();
            MySqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                NoSqlDocument<T> document = JsonConvert.DeserializeObject<NoSqlDocument<T>>(reader.GetString("data"));
                result = document.Data;
            }
            reader.Close();
            connection.Close();
            return result;
        }
        public List<T> GetAll<T>()
        {
            List<T> items = new List<T>();
            string sql = $"SELECT * FROM {schema}.{table};";
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand command = new MySqlCommand(sql, connection);
            connection.Open();
            MySqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                NoSqlDocument<T> document = JsonConvert.DeserializeObject<NoSqlDocument<T>>(reader.GetString("data"));
                items.Add(document.Data);
            }
            reader.Close();
            connection.Close();
            return items;
        }
        public Guid InsertOne<T>(T document)
        {
            return InsertOne<T>(document, Guid.NewGuid());
        }
        public Guid InsertOne<T>(T document, Guid id)
        {
            NoSqlDocument<T> doc = new NoSqlDocument<T>
            {
                Id = id.ToString(),
                Data = document
            };
            string sql = $"INSERT INTO {schema}.{table} (id, data) VALUES('{id.ToString()}', '{JsonConvert.SerializeObject(doc)}');";
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand command = new MySqlCommand(sql, connection);
            connection.Open();
            command.ExecuteNonQuery();
            connection.Close();
            return id;
        }
        public List<Guid> InsertMany<T>(List<T> documents)
        {
            Dictionary<Guid, T> docs = new Dictionary<Guid, T>();
            foreach(T document in documents)
            {
                docs.Add(Guid.NewGuid(), document);
            }
            return InsertMany<T>(docs);
        }
        public List<Guid> InsertMany<T>(Dictionary<Guid, T> documents)
        {
            if (documents.Count == 0) return null;
            StringBuilder sb = new StringBuilder();
            sb.Append($"INSERT INTO {schema}.{table} (id, data) VALUES");
            foreach(KeyValuePair<Guid, T> items in documents)
            {
                NoSqlDocument<T> document = new NoSqlDocument<T> { Id = items.Key.ToString(), Data = items.Value };
                sb.Append($"('{document.Id}', '{JsonConvert.SerializeObject(document.Data)}'), ");
            }
            string sql = sb.ToString().Remove(sb.ToString().Length - 1, 1) + ";";
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand command = new MySqlCommand(sql, connection);
            connection.Open();
            command.ExecuteNonQuery();
            connection.Close();
            return documents.Keys.ToList();
        }
        public void ReplaceOne<T>(T document, Guid id)
        {
            DeleteOne(id);
            InsertOne<T>(document);
        }
        public void UpdateOne<T>(T document, Guid id)
        {
            ReplaceOne<T>(document, id);
        }
        public List<T> Find<T>(string token, int value, SearchComparision comparision = SearchComparision.Equal)
        {
            List<NoSqlDocument<T>> items = FindInternal<T, int>(token, value, comparision);
            List<T> results = new List<T>();
            foreach (NoSqlDocument<T> item in items)
            {
                results.Add(item.Data);
            }
            return results;
        }
        public List<T> Find<T>(string token, float value, SearchComparision comparision = SearchComparision.Equal)
        {
            List<NoSqlDocument<T>> items = FindInternal<T, float>(token, value, comparision);
            List<T> results = new List<T>();
            foreach (NoSqlDocument<T> item in items)
            {
                results.Add(item.Data);
            }
            return results;
        }
        public List<T> Find<T>(string token, string value, SearchComparision comparision = SearchComparision.Equal)
        {
            List<NoSqlDocument<T>> items = FindInternal<T, string>(token, value, comparision);
            List<T> results = new List<T>();
            foreach (NoSqlDocument<T> item in items)
            {
                results.Add(item.Data);
            }
            return results;
        }
        public List<T> Find<T>(string token, bool value, SearchComparision comparision = SearchComparision.Equal)
        {
            List<NoSqlDocument<T>> items = FindInternal<T, bool>(token, value, comparision);
            List<T> results = new List<T>();
            foreach (NoSqlDocument<T> item in items)
            {
                results.Add(item.Data);
            }
            return results;
        }
        public List<T> Find<T>(string token, DateTime value, SearchComparision comparision = SearchComparision.Equal)
        {
            List<NoSqlDocument<T>> items = FindInternal<T, DateTime>(token, value, comparision);
            List<T> results = new List<T>();
            foreach (NoSqlDocument<T> item in items)
            {
                results.Add(item.Data);
            }
            return results;
        }
        private List<NoSqlDocument<T>> FindInternal<T, TValue>(string token, TValue value, SearchComparision comparision = SearchComparision.Equal)
        {
            List<NoSqlDocument<T>> items = new List<NoSqlDocument<T>>();
            string sql = $"SELECT * FROM {schema}.{table};";
            MySqlConnection connection = new MySqlConnection(client.ConnectionString);
            MySqlCommand cmd = new MySqlCommand(sql, connection);
            connection.Open();
            MySqlDataReader reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                try
                {
                    string rawData = reader.GetString("data");
                    JObject obj = JObject.Parse(rawData);
                    NoSqlDocument<T> doc = JsonConvert.DeserializeObject<NoSqlDocument<T>>(rawData);
                    if (value is int)
                    {
                        int a = Convert.ToInt32(value);
                        int b = (int)obj.SelectToken($"Data.{token}");
                        switch (comparision)
                        {
                            case SearchComparision.Equal: if (a == b) items.Add(doc); break;
                            case SearchComparision.NoEqual: if (a != b) items.Add(doc); break;
                            case SearchComparision.EqualToOrGreaterThan: if (a >= b) items.Add(doc); break;
                            case SearchComparision.EqualToOrLessThan: if (a <= b) items.Add(doc); break;
                            case SearchComparision.GreaterThan: if (a > b) items.Add(doc); break;
                            case SearchComparision.LessThan: if (a < b) items.Add(doc); break;
                        }
                    }
                    if (value is float)
                    {
                        float a = Convert.ToSingle(value);
                        float b = (float)obj.SelectToken($"Data.{token}");
                        switch (comparision)
                        {
                            case SearchComparision.Equal: if (a == b) items.Add(doc); break;
                            case SearchComparision.NoEqual: if (a != b) items.Add(doc); break;
                            case SearchComparision.EqualToOrGreaterThan: if (a >= b) items.Add(doc); break;
                            case SearchComparision.EqualToOrLessThan: if (a <= b) items.Add(doc); break;
                            case SearchComparision.GreaterThan: if (a > b) items.Add(doc); break;
                            case SearchComparision.LessThan: if (a < b) items.Add(doc); break;
                        }
                    }
                    if (value is string)
                    {
                        string a = Convert.ToString(value);
                        string b = (string)obj.SelectToken($"Data.{token}");
                        switch (comparision)
                        {
                            case SearchComparision.Equal: if (a == b) items.Add(doc); break;
                            case SearchComparision.NoEqual: if (a != b) items.Add(doc); break;
                        }
                    }
                    if (value is bool)
                    {
                        bool a = Convert.ToBoolean(value);
                        bool b = (bool)obj.SelectToken($"Data.{token}");
                        switch (comparision)
                        {
                            case SearchComparision.Equal: if (a == b) items.Add(doc); break;
                            case SearchComparision.NoEqual: if (a != b) items.Add(doc); break;
                        }
                    }
                    if (value is DateTime)
                    {
                        DateTime a = Convert.ToDateTime(value);
                        DateTime b = (DateTime)obj.SelectToken($"Data.{token}");
                        switch (comparision)
                        {
                            case SearchComparision.Equal: if (a == b) items.Add(doc); break;
                            case SearchComparision.NoEqual: if (a != b) items.Add(doc); break;
                            case SearchComparision.EqualToOrGreaterThan: if (a >= b) items.Add(doc); break;
                            case SearchComparision.EqualToOrLessThan: if (a <= b) items.Add(doc); break;
                            case SearchComparision.GreaterThan: if (a > b) items.Add(doc); break;
                            case SearchComparision.LessThan: if (a < b) items.Add(doc); break;
                        }
                    }
                }
                catch (Exception) { }
            }
            reader.Close();
            connection.Close();
            return items;
        }
        public void FindOneAndDelete<T>(string token, int value, SearchComparision comparision = SearchComparision.Equal)
        {
            FindOneAndDelete<T, int>(token, value, comparision);
        }
        public void FindOneAndDelete<T>(string token, float value, SearchComparision comparision = SearchComparision.Equal)
        {
            FindOneAndDelete<T, float>(token, value, comparision);
        }
        public void FindOneAndDelete<T>(string token, string value, SearchComparision comparision = SearchComparision.Equal)
        {
            FindOneAndDelete<T, string>(token, value, comparision);
        }
        public void FindOneAndDelete<T>(string token, bool value, SearchComparision comparision = SearchComparision.Equal)
        {
            FindOneAndDelete<T, bool>(token, value, comparision);
        }
        public void FindOneAndDelete<T>(string token, DateTime value, SearchComparision comparision = SearchComparision.Equal)
        {
            FindOneAndDelete<T, DateTime>(token, value, comparision);
        }
        private void FindOneAndDelete<T, TValue>(string token, TValue value, SearchComparision comparision = SearchComparision.Equal)
        {
            List<NoSqlDocument<T>> documents = FindInternal<T, TValue>(token, value, comparision);
            if (documents.Count > 0)
            {
                DeleteOne(new Guid(documents.First().Id));
            }
        }
        public T FindOneAndReplace<T>(T replacement, string token, int value, SearchComparision comparision = SearchComparision.Equal)
        {
            return FindOneAndReplace<T, int>(replacement, token, value, comparision);
        }
        public T FindOneAndReplace<T>(T replacement, string token, float value, SearchComparision comparision = SearchComparision.Equal)
        {
            return FindOneAndReplace<T, float>(replacement, token, value, comparision);
        }
        public T FindOneAndReplace<T>(T replacement, string token, string value, SearchComparision comparision = SearchComparision.Equal)
        {
            return FindOneAndReplace<T, string>(replacement, token, value, comparision);
        }
        public T FindOneAndReplace<T>(T replacement, string token, bool value, SearchComparision comparision = SearchComparision.Equal)
        {
            return FindOneAndReplace<T, bool>(replacement, token, value, comparision);
        }
        public T FindOneAndReplace<T>(T replacement, string token, DateTime value, SearchComparision comparision = SearchComparision.Equal)
        {
            return FindOneAndReplace<T, DateTime>(replacement, token, value, comparision);
        }
        private T FindOneAndReplace<T, TValue>(T replaced, string token, TValue value, SearchComparision comparision = SearchComparision.Equal)
        {
            T result = default(T);
            List<NoSqlDocument<T>> documents = FindInternal<T, TValue>(token, value, comparision);

            if (documents.Count > 0)
            {
                Guid id = new Guid(documents.First().Id);
                DeleteOne(id);
                InsertOne(replaced, new Guid(documents.First().Id));
                result = replaced;
            }
            return result;
        }
    }
}