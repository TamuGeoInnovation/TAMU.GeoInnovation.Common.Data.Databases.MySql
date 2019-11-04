using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.TypeConverters;
using USC.GISResearchLab.Common.Databases.TypeConverters.DatabaseTypeConverters;

namespace USC.GISResearchLab.Common.Utils.Databases
{
    public class MySqlParameterUtils
    {

        public static IDbDataParameter BuildDbParameter(string name, Type type, object value)
        {
            OleDbParameter ret = new OleDbParameter(name, type);
            ret.Value = value;
            return ret;
        }

        public static OleDbParameter BuildOleDbParameter(string name, OleDbType type, object value)
        {
            OleDbParameter ret = new OleDbParameter(name, type);
            ret.Value = value;
            return ret;
        }

        public static IDbDataParameter BuildSqlParameter(string name, SqlDbType type, object value)
        {
            return BuildSqlParameter(name, type, value, true);
        }

        public static IDbDataParameter BuildSqlParameter(string name, SqlDbType type, object value, bool useEmptyIfNull)
        {
            string pathToDatabaseDlls = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            return BuildSqlParameter(pathToDatabaseDlls, name, type, value, useEmptyIfNull);
        }

        public static IDbDataParameter BuildSqlParameter(string pathToDatabaseDlls, string name, SqlDbType type, object value, bool useEmptyIfNull)
        {

            AbstractDatabaseDataProviderTypeConverterManager typeConverterSqlServer = (AbstractDatabaseDataProviderTypeConverterManager)DatabaseTypeConverterManagerFactory.GetDatabaseTypeConverterManager(pathToDatabaseDlls, DatabaseType.SqlServer);
            AbstractDatabaseDataProviderTypeConverterManager typeConverterMySql = (AbstractDatabaseDataProviderTypeConverterManager)DatabaseTypeConverterManagerFactory.GetDatabaseTypeConverterManager(pathToDatabaseDlls, DatabaseType.MySql);
            DatabaseSuperDataType superType = typeConverterSqlServer.ToSuperType(type);
            MySqlDbType mysqlType = (MySqlDbType)typeConverterMySql.FromDatabaseSuperDataType(superType);

            MySqlParameter ret = new MySqlParameter(name, mysqlType);
            if (value == null || value == DBNull.Value)
            {
                if (useEmptyIfNull)
                {
                    value = DatabaseDataTypes.GetTypeDefaultValue(superType);
                }
            }

            ret.Value = value;

            return ret;
        }


        public static SqlParameter BuildSqlUdtParameter(string name, string dbTypeName, object value)
        {
            SqlParameter ret = new SqlParameter(name, SqlDbType.Udt) { UdtTypeName = dbTypeName };
            ret.Value = value;
            return ret;
        }
    }
}
