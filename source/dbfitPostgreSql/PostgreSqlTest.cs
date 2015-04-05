namespace dbfit
{
    public class PostgreSqlTest : DatabaseTest
    {
        public PostgreSqlTest()
            : base(new PostgreSqlEnvironment())
        {

        }
    }
}
