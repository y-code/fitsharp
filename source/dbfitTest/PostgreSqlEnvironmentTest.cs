using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using dbfit;
using NUnit.Framework;
using Npgsql;

namespace dbfitTest
{
    [TestFixture]
    class PostgreSqlEnvironmentTest
    {
        [Test]
        public void CheckConnection() {
            var env = new PostgreSqlEnvironment();
            env.Connect("127.0.0.1:5432", "Yasunori", "maido2134", "projectpanel");
            env.CloseConnection();
        }
        [Test]
        public void CheckConnectionWithoutPort()
        {
            var env = new PostgreSqlEnvironment();
            env.Connect("127.0.0.1", "Yasunori", "maido2134", "projectpanel");
            env.CloseConnection();
        }
    }
}
