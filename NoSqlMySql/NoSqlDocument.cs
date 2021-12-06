namespace NoSqlMySql
{
    public class NoSqlDocument<T>
    {
        public string Id { get; set; }
        public T Data { get; set; }
    }
}
