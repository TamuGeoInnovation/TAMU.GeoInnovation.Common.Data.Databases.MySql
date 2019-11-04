using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Core.Utils.Arrays;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Databases.SchemaManagers;
using USC.GISResearchLab.Common.Databases.SqlServer;
using USC.GISResearchLab.Common.Databases.TypeConverters;
using USC.GISResearchLab.Common.Databases.TypeConverters.DataProviderTypeConverters;
using USC.GISResearchLab.Common.Utils.Databases.TableDefinitions;

namespace USC.GISResearchLab.Common.Databases.MySql
{
    public class MySqlSchemaManager : AbstractSchemaManager
    {

        public MySqlSchemaManager()
        {
            DataProviderType = DataProviderType.MySql;
            DatabaseType = DatabaseType.MySql;
            QueryManager = new QueryManager(DataProviderType, DatabaseType);
        }

        public MySqlSchemaManager(string connectionString)
        {
            DataProviderType = DataProviderType.MySql;
            DatabaseType = DatabaseType.MySql;
            ConnectionString = connectionString;
            QueryManager = new QueryManager(DataProviderType, DatabaseType, ConnectionString);
        }

        public override void CreateDatabase()
        {
            throw new NotImplementedException();
        }

        public override TableDefinition GetTableDefinition(string table)
        {
            TableDefinition ret = null;
            try
            {
                TableColumn[] tableColumns = GetColumns(table);
                if (tableColumns != null)
                {
                    if (ret == null)
                    {
                        ret = new TableDefinition(DataProviderType, table);
                    }
                    ret.TableColumns = tableColumns;
                }
            }
            catch (Exception ex)
            {

                string msg = "Error getting table definition: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;
        }


        public override void AddColumnsToTable(string tableName, string[] columnNames, DatabaseSuperDataType[] dataTypes)
        {
            for (int i = 0; i < columnNames.Length; i++)
            {
                AddColumnToTable(tableName, columnNames[i], dataTypes[i]);
            }
        }

        public override void AddColumnToTable(string tableName, string columnName, DatabaseSuperDataType dataType)
        {
            AddColumnToTable(tableName, columnName, dataType, true, 0, 0);
        }

        public override void AddColumnToTable(string tableName, string columnName, DatabaseSuperDataType dataType, bool nullable, int maxLength, int precision)
        {
            throw new NotImplementedException();
        }

        public override string BuildCreateTableStatement(TableDefinition tableDefinition)
        {
            string ret = "";
            if (tableDefinition != null)
            {
                ret += " CREATE TABLE " + tableDefinition.Name + " (";

                for (int i = 0; i < tableDefinition.TableColumns.Length; i++)
                {

                    TableColumn column = tableDefinition.TableColumns[i];
                    if (i > 0)
                    {
                        ret += ", ";
                    }

                    ret += " `" + column.Name + "`";
                    ret += " " + DatabaseTypeConverter.GetTypeAsString(column.DatabaseSuperDataType);
                    if (column.Length > 0 || column.DatabaseSuperDataType == DatabaseSuperDataType.String || column.DatabaseSuperDataType == DatabaseSuperDataType.VarChar)
                    {
                        if (column.DatabaseSuperDataType != DatabaseSuperDataType.Double)
                        {
                            if (column.Length > 0)
                            {
                                ret += "(" + column.Length + ")";
                            }
                            else
                            {
                                ret += "(255)";
                            }
                        }
                    }

                    if (column.IsPrimaryKey)
                    {
                        ret += " PRIMARY KEY ";
                    }

                    if (!column.IsNullable)
                    {
                        ret += " NOT NULL ";
                    }

                    if (column.DefaultValue != null)
                    {
                        ret += " " + column.DefaultValue.ToString();
                    }
                }
                ret += ")";
            }
            return ret;
        }

        public static string BuildConnectionString(string dataSource, string catalog, string userName, string password)
        {
            return "Server=" + dataSource + ";Uid=" + userName + ";Pwd=" + password + ";Database=" + catalog + ";";
        }

        public static bool DatabaseIsValid(string dataSource, string catalog, string userName, string password)
        {
            bool ret = false;
            MySqlConnection conn = null;
            try
            {
                conn = GetConnection(dataSource, catalog, userName, password);
                conn.Open();
                ret = true;
            }
            catch (Exception)
            {
                if (conn != null)
                {
                    if (conn.State == ConnectionState.Open)
                    {
                        conn.Close();
                    }
                }
            }
            finally
            {
                if (ret)
                {
                    if (conn != null)
                    {
                        conn.Close();
                    }
                }
            }
            return ret;
        }

        public static MySqlConnection GetConnection(string dsnName)
        {
            MySqlConnection ret = null;
            try
            {
                ret = new MySqlConnection("jdbc:odbc:" + dsnName);
            }
            catch (Exception ex)
            {
                if (ret != null)
                {
                    ret.Close();
                }
                string msg = "Error getting mysql database connection: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;
        }

        public static MySqlConnection GetConnection(string dataSource, string catalog, string userName, string password)
        {
            MySqlConnection ret = null;
            try
            {
                ret = new MySqlConnection(BuildConnectionString(dataSource, catalog, userName, password));
            }
            catch (Exception ex)
            {

                string msg = "Error getting sql server database connection: " + ex.Message;
            }
            return ret;
        }


        public ArrayList GetTablesAsArrayList()
        {
            ArrayList ret = null;

            try
            {
                DataTable schemaTable = GetTablesAsDataTable();
                if (schemaTable != null)
                {
                    ret = new ArrayList();
                    for (int i = 0; i < schemaTable.Rows.Count; i++)
                    {
                        ret.Add(schemaTable.Rows[i].ItemArray[0].ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error getting table names: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;
        }

        public override TableColumn[] GetColumns(string tableName)
        {
            return GetColumns(tableName, true);
        }

        // this is partly from http://www.codeproject.com/KB/database/AbdMySqlSchema.aspx
        public override TableColumn[] GetColumns(string tableName, bool shouldOpenAndClose)
        {

            TableColumn[] ret = null;

            try
            {
                if (shouldOpenAndClose)
                {
                    Connection.Open();
                }

                MySqlCommand cmd = new MySqlCommand("SHOW COLUMNS IN " + tableName, (MySqlConnection)Connection);
                MySqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    // This query returns the name of column, Type,
                    // wherther it's Null, whether it's primary Key,
                    // the default value, and extra info such as 
                    // whether it's autoincrement or not           

                    string name = Convert.ToString(reader[0]);
                    string dbType = Convert.ToString(reader[1]);
                    string isNullable = Convert.ToString(reader[2]);
                    string isPrimaryKey = Convert.ToString(reader[3]);
                    object defaultValue = reader[4];
                    string extra = Convert.ToString(reader[5]);

                    string typeString = dbType;
                    string length = "";

                    if (dbType.IndexOf('(') != -1)
                    {
                        typeString = dbType.Substring(0, dbType.IndexOf('(')).Trim();
                        length = dbType.Substring(dbType.IndexOf('(') + 1, dbType.IndexOf(')') - dbType.IndexOf('(') - 1).Trim();
                    }

                    MySqlDbType type = new MySqlTypeConverter().FromShowColumnsString(typeString);
                    IDataProviderTypeConverterManager typeConverter = DataProviderTypeConverterManagerFactory.GetDataProviderTypeConverterManager(PathToDatabaseDLLs, DataProviderType);
                    DatabaseSuperDataType databaseSuperDataType = typeConverter.ToSuperType(type);
                    TableColumn column = new TableColumn(name, databaseSuperDataType, defaultValue);

                    if (type != MySqlDbType.Int32 && type != MySqlDbType.Datetime)
                    {
                        if (length != String.Empty)
                        {
                            column.Length = Convert.ToInt32(length);
                        }
                    }

                    if (String.Compare(isNullable, "YES", true) != 0)
                    {
                        column.IsNullable = false;
                    }
                    else
                    {
                        column.IsNullable = true;
                    }


                    if (String.Compare(isPrimaryKey, "PRI", true) == 0)
                    {
                        column.IsPrimaryKey = true;
                    }

                    if (String.Compare(extra, "auto_increment", true) == 0)
                    {
                        column.IsAutoIncrement = true;
                    }

                    if (ret == null)
                    {
                        ret = new TableColumn[0];
                    }

                    TableColumn[] newList = new TableColumn[ret.Length + 1];
                    for (int j = 0; j < ret.Length; j++)
                    {
                        newList[j] = ret[j];
                    }

                    newList[newList.Length - 1] = column;
                    ret = newList;
                }
            }
            catch (Exception ex)
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }

                string msg = "Error getting table columns: " + ex.Message;
                throw new Exception(msg, ex);
            }
            finally
            {
                if (shouldOpenAndClose)
                {
                    if (Connection != null)
                    {
                        if (Connection.State != ConnectionState.Closed)
                        {
                            Connection.Close();
                        }
                    }
                }
            }
            return ret;
        }

        public string GetCreateTableStatement(string tableName)
        {

            string ret = "";

            try
            {

                MySqlCommand cmd = new MySqlCommand("show create table " + tableName, (MySqlConnection)Connection);
                MySqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    ret += "drop table if exists " + tableName + "\r\n";
                    ret += Convert.ToString((reader[1]));
                    ret += ";\r\n";
                }
                reader.Close();

            }
            catch (Exception ex)
            {
                string msg = "Error getting create table: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;
        }

        public override string[] GetDatabases()
        {
            return GetDatabases(false, DatabaseNameListingOptions.AllDatabases);
        }

        public override string[] GetDatabases(bool shouldOpenAndClose)
        {
            return GetDatabases(shouldOpenAndClose, DatabaseNameListingOptions.AllDatabases);
        }

        public override string[] GetDatabases(DatabaseNameListingOptions opt)
        {
            return GetDatabases(false, opt);
        }

        public override string[] GetDatabases(bool shouldOpenAndClose, DatabaseNameListingOptions opt)
        {
            string[] ret = null;
            DataTable dataTable = GetDatabasesAsDataTable();

            if (dataTable != null && dataTable.Rows.Count > 1)
            {
                List<string> names = new List<string>();

                foreach (DataRow dataRow in dataTable.Rows)
                {
                    names.Add(Convert.ToString(dataRow[0]));
                }

                ret = names.ToArray();
            }

            return ret;
        }

        public override DataTable GetDatabasesAsDataTable()
        {
            return GetDatabasesAsDataTable(false, DatabaseNameListingOptions.AllDatabases);
        }

        public override DataTable GetDatabasesAsDataTable(bool shouldOpenAndClose)
        {
            return GetDatabasesAsDataTable(shouldOpenAndClose, DatabaseNameListingOptions.AllDatabases);
        }

        public override DataTable GetDatabasesAsDataTable(bool shouldOpenAndClose, DatabaseNameListingOptions opt)
        {

            DataTable ret = null;

            try
            {

                string sql = "";
                sql += " show databases ";

                MySqlCommand cmd = new MySqlCommand(sql);
                //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("organizationName", SqlDbType.VarChar, organizationName));
                //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("organizationGuid", SqlDbType.VarChar, organizationGuid));

                IQueryManager qm = QueryManager;
                qm.AddParameters(cmd.Parameters);
                ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);

            }
            catch (Exception ex)
            {
                string msg = "Error getting GetDatabasesAsDataTable: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;

        }

        public override string[] GetTables()
        {
            return GetTables(true);
        }

        public override string[] GetTables(bool shouldOpenAndClose)
        {
            string[] ret = null;
            try
            {
                DataTable dataTable = GetTablesAsDataTable(shouldOpenAndClose);
                if (dataTable != null)
                {
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        string table = Convert.ToString(dataTable.Rows[i][0]);
                        if (table != String.Empty)
                        {
                            ret = (string[])ArrayUtils.Add(ret, Convert.ToString(dataTable.Rows[i][0]));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error getting tables: " + ex.Message;
                throw new Exception(msg, ex);

            }
            return ret;
        }

        public override DataTable GetTablesAsDataTable()
        {
            return GetTablesAsDataTable(true);
        }

        public override DataTable GetTablesAsDataTable(bool shouldOpenAndClose)
        {
            throw new Exception("Must call GetTablesAsDataTable with database name");
        }

        public override DataTable GetTablesAsDataTable(string databaseName, bool shouldOpenAndClose)
        {
            DataTable ret = null;
            string sql = "";
            sql += "SHOW TABLES FROM " + databaseName;

            MySqlCommand cmd = new MySqlCommand(sql);
            //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("organizationName", SqlDbType.VarChar, organizationName));
            //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("organizationGuid", SqlDbType.VarChar, organizationGuid));

            IQueryManager qm = QueryManager;
            qm.AddParameters(cmd.Parameters);
            ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);

            //DataTable ret = null;
            //try
            //{
            //    if (shouldOpenAndClose)
            //    {
            //        Connection.Open();
            //    }

            //    MySqlDataAdapter da = new MySqlDataAdapter("SHOW TABLES FROM " + databaseName, ((MySqlConnection)Connection));
            //    ret = new DataTable();
            //    da.Fill(ret);

            //}
            //catch (Exception ex)
            //{
            //    if (shouldOpenAndClose)
            //    {
            //        if (Connection != null)
            //        {
            //            if (Connection.State != ConnectionState.Closed)
            //            {
            //                Connection.Close();
            //            }
            //        }
            //    }

            //    string msg = "Error getting table schema: " + ex.Message;
            //    throw new Exception(msg, ex);
            //}
            //finally
            //{
            //    if (shouldOpenAndClose)
            //    {
            //        if (Connection != null)
            //        {
            //            if (Connection.State != ConnectionState.Closed)
            //            {
            //                Connection.Close();
            //            }
            //        }
            //    }

            //}

            return ret;
        }

        public override void RemoveTableFromDatabase(string tableName)
        {
            try
            {
                string sql = "drop table if exists " + tableName + ";";
                QueryManager.ExecuteNonQuery(CommandType.Text, sql);
            }
            catch (Exception ex)
            {
                string msg = "Error removing table: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

        public override void RemoveIndexFromTable(string tableName, string indexName)
        {
            throw new NotImplementedException();
        }

        public override void RemoveSpatialIndexFromTable(string tableName, string indexName)
        {
            throw new NotImplementedException();
        }

        public override void RemoveConstraintFromTable(string tableName, string constraintName)
        {
            throw new NotImplementedException();
        }

        public override string[] GetTableIndexes(string tableName)
        {
            throw new NotImplementedException();
        }

        public override string[] GetTableIndexes(string tableName, bool shouldOpenAndClose)
        {
            throw new NotImplementedException();
        }

        public override string[] GetTableSpatialIndexes(string tableName)
        {
            throw new NotImplementedException();
        }

        public override string[] GetTableSpatialIndexes(string tableName, bool shouldOpenAndClose)
        {
            throw new NotImplementedException();
        }

        public override string GetTableClusteredIndex(string tableName)
        {
            throw new NotImplementedException();
        }

        public override string GetTableClusteredIndex(string tableName, bool shouldOpenAndClose)
        {
            throw new NotImplementedException();
        }

        public override void AddGeogIndexToDatabase(string tableName)
        {
            //Do Nothing
            // throw new NotImplementedException();
        }

        public override void AddGeogIndexToDatabase(string tableName, bool shouldOpenCloseConnection)
        {
            //Do Nothing
            // throw new NotImplementedException();
        }
    }
}
