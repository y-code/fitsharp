using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Text;
using dbfit.util;
using Npgsql;
using NpgsqlTypes;

namespace dbfit
{
    /// <summary>
    /// Implementation of IDbEnvironment that uses Microsoft's ADO.NET driver for Oracle
    /// </summary>

    public class PostgreSqlEnvironment : AbstractDbEnvironment
    {
        protected override String GetConnectionString(String dataSource, String username, String password) {
            int sepIdx = dataSource.IndexOf(':');
            String address;
            String port;
            if (sepIdx > 0) {
                address = dataSource.Substring(0, sepIdx);
                port = dataSource.Substring(sepIdx + 1, dataSource.Length - (sepIdx + 1));
            }
            else {
                address = dataSource;
                port = "5432";
            }
            return String.Format("Server={0};Port={1};User Id={2};Password={3};", address, port, username, password);
        }
        protected override String GetConnectionString(String dataSource, String username, String password, String databaseName)
        {
            return GetConnectionString(dataSource, username, password) + String.Format("Database={0};", databaseName);
        }
        private Regex paramNames = new Regex(":([A-Za-z0-9_]+)");
        protected override Regex ParamNameRegex { get { return paramNames; } }

        private static DbProviderFactory dbp = DbProviderFactories.GetFactory("Npgsql");
        public override DbProviderFactory DbProviderFactory
        {
            get { return dbp; }
        }
        public override Dictionary<String, DbParameterAccessor> GetAllProcedureParameters(String procName)
        {
            String[] qualifiers = NameNormaliser.NormaliseName(procName).Split('.');
            // The column ordinal_position is numbered from 1, so that it will put 0 for return type.
            String qry0 = @"select parameter_name, data_type, parameter_mode, ordinal_position " +
                "from information_schema.parameters " +
                "where ";
            if (qualifiers.Length == 3)
            {
                qry0 += " specific_catalog=:0 and specific_schema=:1 and specific_name=:2 ";
            }
            else if (qualifiers.Length == 2)
            {
                qry0 += @" specific_schema=:0 and specific_name=:1 ";
            }
            else
            {
                qry0 += @"specific_name=:0 ";
            }
            String qry1 = @"select null parameter_name, data_type, parameter_mode, 0 ordinal_position " +
                "from information_schema.routines " +
                "where ";
            if (qualifiers.Length == 3)
            {
                qry1 += " specific_catalog=:0 and specific_schema=:1 and specific_name=:2 ";
            }
            else if (qualifiers.Length == 2)
            {
                qry1 += @" specific_schema=:0 and specific_name=:1 ";
            }
            else
            {
                qry1 += @"specific_name=:0 ";
            }


            String qry = @"select * from ( " + qry0 + @"union all " + qry1 + @") " +
                "order by ordinal_position ";
            //Console.WriteLine(qry);
            Dictionary<String, DbParameterAccessor> res = ReadIntoParams(qualifiers, qry);
            if (res.Count == 0) throw new ApplicationException("Cannot read list of parameters for " + procName + " - check spelling and access privileges");
            return res;
        }
        public override Dictionary<String, DbParameterAccessor> GetAllColumns(String tableOrViewName)
        {
            String[] qualifiers = NameNormaliser.NormaliseName(tableOrViewName).Split('.');
            String qry = @" select column_name, data_type, 'IN' as direction, ordinal_position from information_schema.columns " +
                "where ";
            if (qualifiers.Length == 3)
            {
                qry += " table_catalog=:0 and table_schema=:1 and table_name=:2 ";
            }
            else if (qualifiers.Length == 2)
            {
                qry += @" table_schema=:0 and table_name=:1 ";
            }
            else
            {
                qry += @"table_name=:0 ";
            }
            qry += " order by ordinal_position ";
            Dictionary<String, DbParameterAccessor> res = ReadIntoParams(qualifiers, qry);
            if (res.Count == 0) throw new ApplicationException("Cannot read list of columns for " + tableOrViewName + " - check spelling and access privileges");
            return res;
        }

        private Dictionary<string, DbParameterAccessor> ReadIntoParams(String[] queryParameters, String query)
        {
            var dc = new NpgsqlCommand {
                Transaction = (NpgsqlTransaction)CurrentTransaction,
                CommandText = query,
                CommandType = CommandType.Text
            };
            for (int i = 0; i < queryParameters.Length; i++)
                AddInput(dc, ParameterPrefix + i, queryParameters[i].ToUpper());
            DbDataReader reader = dc.ExecuteReader();
            Dictionary<String, DbParameterAccessor>
                allParams = new Dictionary<string, DbParameterAccessor>();
            int position = 0;
            while (reader.Read())
            {

                String paramName = (reader.IsDBNull(0)) ? null : reader.GetString(0);
                String dataType = reader.GetString(1);
                //int length = (reader.IsDBNull(2)) ? 0 : reader.GetInt32(2);
                String direction = reader.GetString(2);
                NpgsqlParameter dp = dc.CreateParameter();
                if (paramName != null)
                {
                    dp.ParameterName = paramName;
                    dp.SourceColumn = paramName;
                    dp.Direction = GetParameterDirection(direction);
                }
                else
                {
                    dp.Direction = ParameterDirection.ReturnValue;
                }

                dp.NpgsqlDbType = GetDbType(dataType);
                if (!ParameterDirection.Input.Equals(dp.Direction) || typeof(String) == GetDotNetType(dataType))
                    dp.Size = 4000;
                allParams[NameNormaliser.NormaliseName(paramName)] =
                    new DbParameterAccessor(dp, GetDotNetType(dataType), position++, dataType);
            }
            return allParams;
        }

        #region Data Type Mapping between PostgreSQL and Npgsql

//abstime	Abstime	NpgsqlTime
//bit	Bit	BitArray
//bit varying	Bytea	BitArray
//bytea	Bytea	BitArray
//boolean	Boolean	Boolean
//money	Money	Decimal
//numeric	Numeric	Decimal
//double precision	Double	Double
//uuid	Uuid	Guid
//bigint	Bigint	Int64
//box	Box	NpgsqlBox
//circle	Circle	NpgsqlCircle
//date	Date	NpgsqlDate
//cidr	Inet	NpgsqlInet
//inet	Inet	NpgsqlInet
//interval	Interval	NpgsqlInterval
//lseg	LSeg	NpgsqlLSeg
//macaddr	MacAddr	NpgsqlMacAddress
//path	Path	NpgsqlPath
//point	Point	NpgsqlPoint
//polygon	Polygon	NpgsqlPolygon
//tinterval	Abstime	NpgsqlTime
//time with time zone	Time	NpgsqlTime
//time without time zone	Time	NpgsqlTime
//timestamp with time zone	Timestamp	NpgsqlTimeStamp
//timestamp without time zone	TimestampTZ	NpgsqlTimeStampTZ
//real	Real	Single
//"char"	Char	String
//character	Char	String
//json	Json	String
//name	Name	String
//refcursor	Refcursor	String
//text	Text	String
//character varying	Varchar	String
//xml	Xml	String
//smallint	Smallint	Int16
//bigserial	Integer	Int32
//cid	Integer	Int32
//integer	Integer	Int32
//oid	Integer	Int32
//regclass	Integer	Int32
//regconfig	Integer	Int32
//regdictionary	Integer	Int32
//regoper	Integer	Int32
//regoperator	Integer	Int32
//regproc	Integer	Int32
//regprocedure	Integer	Int32
//regtype	Integer	Int32
//serial	Integer	Int32
//smallserial	Integer	Int32
//xid	Integer	Int32
//line	Line	
//ARRAY		
//"any"		
//aclitem		
//anyarray		
//anyelement		
//anyenum		
//anynonarray		
//anyrange		
//cstring		
//daterange		
//event_trigger		
//fdw_handler		
//gtsvector		
//int4range		
//int8range		
//internal		
//language_handler		
//numrange		
//opaque		
//pg_attribute		
//pg_node_tree		
//pg_type		
//record		
//reltime		
//smgr		
//tid		
//trigger		
//tsquery		
//tsrange		
//tstzrange		
//tsvector		
//txid_snapshot		
//unknown		
//void		

        private class DbTypeMapping {
            public String PgType { get; private set; }
            public NpgsqlDbType NpgsqlType { get; private set; }
            public Type DotNetType { get; private set; }

            public DbTypeMapping(String pgType, NpgsqlDbType npgsqlType, Type dotNetType) {
                PgType = pgType;
                NpgsqlType = npgsqlType;
                DotNetType = dotNetType;
            }
        }
        private static readonly Dictionary<String, DbTypeMapping> dbTypeMappings = new Dictionary<String, DbTypeMapping> {
            { "abstime", new DbTypeMapping("abstime", NpgsqlDbType.Abstime, typeof(NpgsqlTime)) },
            { "bit", new DbTypeMapping("bit", NpgsqlDbType.Bit, typeof(BitArray)) },
            { "bit varying", new DbTypeMapping("bit varying", NpgsqlDbType.Bytea, typeof(BitArray)) },
            { "bytea", new DbTypeMapping("bytea", NpgsqlDbType.Bytea, typeof(BitArray)) },
            { "boolean", new DbTypeMapping("boolean", NpgsqlDbType.Boolean, typeof(Boolean)) },
            { "money", new DbTypeMapping("money", NpgsqlDbType.Money, typeof(Decimal)) },
            { "numeric", new DbTypeMapping("numeric", NpgsqlDbType.Numeric, typeof(Decimal)) },
            { "double precision", new DbTypeMapping("double precision", NpgsqlDbType.Double, typeof(Double)) },
            { "uuid", new DbTypeMapping("uuid", NpgsqlDbType.Uuid, typeof(Guid)) },
            { "bigint", new DbTypeMapping("bigint", NpgsqlDbType.Bigint, typeof(Int64)) },
            { "box", new DbTypeMapping("box", NpgsqlDbType.Box, typeof(NpgsqlBox)) },
            { "circle", new DbTypeMapping("circle", NpgsqlDbType.Circle, typeof(NpgsqlCircle)) },
            { "date", new DbTypeMapping("date", NpgsqlDbType.Date, typeof(NpgsqlDate)) },
            { "cidr", new DbTypeMapping("cidr", NpgsqlDbType.Inet, typeof(NpgsqlInet)) },
            { "inet", new DbTypeMapping("inet", NpgsqlDbType.Inet, typeof(NpgsqlInet)) },
            { "interval", new DbTypeMapping("interval", NpgsqlDbType.Interval, typeof(NpgsqlInterval)) },
            { "lseg", new DbTypeMapping("lseg", NpgsqlDbType.LSeg, typeof(NpgsqlLSeg)) },
            { "macaddr", new DbTypeMapping("macaddr", NpgsqlDbType.MacAddr, typeof(NpgsqlMacAddress)) },
            { "path", new DbTypeMapping("path", NpgsqlDbType.Path, typeof(NpgsqlPath)) },
            { "point", new DbTypeMapping("point", NpgsqlDbType.Point, typeof(NpgsqlPoint)) },
            { "polygon", new DbTypeMapping("polygon", NpgsqlDbType.Polygon, typeof(NpgsqlPolygon)) },
            { "tinterval", new DbTypeMapping("tinterval", NpgsqlDbType.Abstime, typeof(NpgsqlTime)) },
            { "time with time zone", new DbTypeMapping("time with time zone", NpgsqlDbType.Time, typeof(NpgsqlTime)) },
            { "time without time zone", new DbTypeMapping("time without time zone", NpgsqlDbType.Time, typeof(NpgsqlTime)) },
            { "timestamp with time zone", new DbTypeMapping("timestamp with time zone", NpgsqlDbType.Timestamp, typeof(NpgsqlTimeStamp)) },
            { "timestamp without time zone", new DbTypeMapping("timestamp without time zone", NpgsqlDbType.TimestampTZ, typeof(NpgsqlTimeStampTZ)) },
            { "real", new DbTypeMapping("real", NpgsqlDbType.Real, typeof(Single)) },
            { "\"char\"", new DbTypeMapping("“char”", NpgsqlDbType.Char, typeof(String)) },
            { "character", new DbTypeMapping("character", NpgsqlDbType.Char, typeof(String)) },
            { "json", new DbTypeMapping("json", NpgsqlDbType.Json, typeof(String)) },
            { "name", new DbTypeMapping("name", NpgsqlDbType.Name, typeof(String)) },
            { "refcursor", new DbTypeMapping("refcursor", NpgsqlDbType.Refcursor, typeof(String)) },
            { "text", new DbTypeMapping("text", NpgsqlDbType.Text, typeof(String)) },
            { "character varying", new DbTypeMapping("character varying", NpgsqlDbType.Varchar, typeof(String)) },
            { "xml", new DbTypeMapping("xml", NpgsqlDbType.Xml, typeof(String)) },
            { "smallint", new DbTypeMapping("smallint", NpgsqlDbType.Smallint, typeof(Int16)) },
            { "bigserial", new DbTypeMapping("bigserial", NpgsqlDbType.Integer, typeof(Int32)) },
            { "cid", new DbTypeMapping("cid", NpgsqlDbType.Integer, typeof(Int32)) },
            { "integer", new DbTypeMapping("integer", NpgsqlDbType.Integer, typeof(Int32)) },
            { "oid", new DbTypeMapping("oid", NpgsqlDbType.Integer, typeof(Int32)) },
            { "regclass", new DbTypeMapping("regclass", NpgsqlDbType.Integer, typeof(Int32)) },
            { "regconfig", new DbTypeMapping("regconfig", NpgsqlDbType.Integer, typeof(Int32)) },
            { "regdictionary", new DbTypeMapping("regdictionary", NpgsqlDbType.Integer, typeof(Int32)) },
            { "regoper", new DbTypeMapping("regoper", NpgsqlDbType.Integer, typeof(Int32)) },
            { "regoperator", new DbTypeMapping("regoperator", NpgsqlDbType.Integer, typeof(Int32)) },
            { "regproc", new DbTypeMapping("regproc", NpgsqlDbType.Integer, typeof(Int32)) },
            { "regprocedure", new DbTypeMapping("regprocedure", NpgsqlDbType.Integer, typeof(Int32)) },
            { "regtype", new DbTypeMapping("regtype", NpgsqlDbType.Integer, typeof(Int32)) },
            { "serial", new DbTypeMapping("serial", NpgsqlDbType.Integer, typeof(Int32)) },
            { "smallserial", new DbTypeMapping("smallserial", NpgsqlDbType.Integer, typeof(Int32)) },
            { "xid", new DbTypeMapping("xid", NpgsqlDbType.Integer, typeof(Int32)) },
        };

        #endregion

        private static string NormaliseTypeName(string dataType)
        {
            dataType = dataType.ToLower().Trim();
            //int idx = dataType.IndexOf(" ");
            //if (idx >= 0) dataType = dataType.Substring(0, idx);
            int idx = dataType.IndexOf("(", StringComparison.Ordinal);
            if (idx >= 0) dataType = dataType.Substring(0, idx);
            return dataType;
        }
        private static NpgsqlDbType GetDbType(String dataType)
        {
            dataType = NormaliseTypeName(dataType);

            if (dbTypeMappings.ContainsKey(dataType))
                throw new NotSupportedException("Type " + dataType + " is not supported");

            return dbTypeMappings[dataType].NpgsqlType;
        }
        private static Type GetDotNetType(String dataType)
        {
            dataType = NormaliseTypeName(dataType);
            
            if (dbTypeMappings.ContainsKey(dataType))
                throw new NotSupportedException("Type " + dataType + " is not supported");

            return dbTypeMappings[dataType].DotNetType;
        }
        private static ParameterDirection GetParameterDirection(String direction)
        {
            if ("IN".Equals(direction)) return ParameterDirection.Input;
            if ("OUT".Equals(direction)) return ParameterDirection.Output;
            if ("INOUT".Equals(direction)) return ParameterDirection.InputOutput;
            throw new NotSupportedException("Direction " + direction + " is not supported");
        }

        public override String BuildInsertCommand(String tableName, DbParameterAccessor[] accessors)
        {
            StringBuilder sb = new StringBuilder("insert into ");
            sb.Append(tableName).Append("(");
            String comma = "";
            String retComma = "";

            StringBuilder values = new StringBuilder();
            StringBuilder retNames = new StringBuilder();

            foreach (DbParameterAccessor accessor in accessors)
            {
                if (!accessor.IsBoundToCheckOperation)
                {
                    sb.Append(comma);
                    values.Append(comma);
                    sb.Append(accessor.DbParameter.SourceColumn);
                    values.Append(ParameterPrefix).Append(accessor.DbParameter.ParameterName);
                    comma = ",";
                }
                else
                {
                    retNames.Append(retComma);
                    retNames.Append(accessor.DbParameter.SourceColumn);
                    retComma = ",";
                }
            }
            sb.Append(") values (");
            sb.Append(values);
            sb.Append(")");
            if (retNames.Length > 0)
            {
                sb.Append(" returning ").Append(retNames);
            }
            return sb.ToString();
        }
        public override int GetExceptionCode(Exception dbException)
        {
            if (dbException is DbException) // It includes NpgsqlException.
                return ((DbException)dbException).ErrorCode;
            else return 0;
        }
        public override String ParameterPrefix
        {
            get { return ":"; }
        }
        public override bool SupportsReturnOnInsert { get { return true; } }
        public override String IdentitySelectStatement(string tableName) { throw new ApplicationException("Oracle supports return on insert"); }

    }
}
