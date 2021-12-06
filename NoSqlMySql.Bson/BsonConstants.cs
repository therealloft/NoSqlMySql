using System;

namespace NoSqlMySql.Bson
{
    public static class BsonConstants
    {
        private static readonly long dateTimeMaxValueMillisecondsSinceEpoch;
        private static readonly long dateTimeMinValueMillisecondsSinceEpoch;
        private static readonly DateTime unixEpoch;

        static BsonConstants()
        {
            unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            dateTimeMaxValueMillisecondsSinceEpoch = (DateTime.MaxValue - unixEpoch).Ticks / 10000;
            dateTimeMinValueMillisecondsSinceEpoch = (DateTime.MinValue - unixEpoch).Ticks / 10000;
        }
        public static long DateTimeMaxValueMillisecondsSinceEpoch {
            get { return dateTimeMaxValueMillisecondsSinceEpoch; }
        }
        public static long DateTimeMinValueMillisecondsSinceEpoch {
            get { return dateTimeMinValueMillisecondsSinceEpoch; }
        }
        public static DateTime UnixEpoch {
            get { return unixEpoch; }
        }
    }
}
