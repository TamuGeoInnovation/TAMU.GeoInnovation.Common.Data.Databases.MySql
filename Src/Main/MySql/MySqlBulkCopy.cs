using Microsoft.SqlServer.Types;
using MySql.Data.MySqlClient;
using SQLSpatialTools;
using System;
using System.Data;
using USC.GISResearchLab.Common.Core.Databases.BulkCopys;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Utils.Databases;

namespace USC.GISResearchLab.Common.Core.Databases.MySql
{
    public class MySqlBulkCopy : AbstractBulkCopy
    {

        public MySqlBulkCopy(IQueryManager queryManager)
        {
            QueryManager = queryManager;
        }


        public MySqlBulkCopy(DatabaseType databaseType, MySqlConnection conn)
        {
            DatabaseType = databaseType;
            Connection = conn;
            QueryManager = new QueryManager(DataProviderType.OleDb, DatabaseType, Connection.ConnectionString);
        }

        public override void Close()
        {
            throw new NotImplementedException();
        }

        public override void GenerateColumnMappings()
        {
            //throw new NotImplementedException();
        }

        public override void GenerateColumnMappings(string[] excludeColumns)
        {
            //throw new NotImplementedException();
        }

        public override void WriteToServer(DataRow[] rows)
        {
            throw new NotImplementedException();
        }

        public override void WriteToServer(DataTable dataTable)
        {
            WriteToServer(dataTable.CreateDataReader());
        }

        public override void WriteToServer(DataTable dataTable, DataRowState dataRowState)
        {
            WriteToServer(dataTable.CreateDataReader());
        }

        public override void WriteToServer(IDataReader dataReader)
        {
            QueryManager.Connection.Open();
            while (dataReader.Read())
            {

                bool shouldInsert = false;

                if (ShouldSkipExistingRecords)
                {
                    object value = dataReader.GetValue(dataReader.GetOrdinal(ExistingRecordIdField));
                    string sql = "";
                    sql += " SELECT " + ExistingRecordIdField + " FROM " + DestinationTableName;
                    sql += " WHERE ";
                    sql += "`" + ExistingRecordIdField + "`" + "=?" + ExistingRecordIdField;

                    IDbCommand cmd = QueryManagerFactory.GetCommand(QueryManager.PathToDatabaseDLLs, QueryManager.ProviderType);
                    cmd.CommandText = sql;
                    cmd.Parameters.Add(MySqlParameterUtils.BuildSqlParameter(ExistingRecordIdField, SqlDbType.VarChar, value));

                    QueryManager.AddParameters(cmd.Parameters);
                    string exists = QueryManager.ExecuteScalarString(CommandType.Text, cmd.CommandText, false);

                    if (String.IsNullOrEmpty(exists))
                    {
                        shouldInsert = true;
                    }
                    else
                    {
                        shouldInsert = false;
                    }

                }
                else
                {
                    shouldInsert = true;
                }

                if (shouldInsert)
                {

                    string sql = "";
                    sql += "INSERT INTO " + DestinationTableName;
                    sql += "(";

                    int i = 0;
                    foreach (DataRow dataRow in SchemaDataTable.Rows)
                    {
                        if (i > 0)
                        {
                            sql += ",";
                        }

                        string columnName = (string)dataRow[0];
                        sql += "`" + columnName + "`";
                        i++;
                    }

                    sql += ") ";
                    sql += "VALUES ";
                    sql += "(";

                    int j = 0;
                    foreach (DataRow dataRow in SchemaDataTable.Rows)
                    {
                        if (j > 0)
                        {
                            sql += ",";
                        }

                        Type type = (Type)dataRow["DataType"];
                        if (type == typeof(SqlGeometry) || type == typeof(SqlGeography))
                        {
                            //sql += "GeomFromText(";
                            sql += "GeomFromWKB(";
                        }


                        string columnName = (string)dataRow[0];
                        sql += "?" + columnName;

                        if (type == typeof(SqlGeometry) || type == typeof(SqlGeography))
                        {
                            sql += ")";
                        }
                        j++;
                    }

                    sql += ") ";

                    //IDbCommand cmd = new MySqlCommand(sql);
                    IDbCommand cmd = QueryManagerFactory.GetCommand(QueryManager.PathToDatabaseDLLs, QueryManager.ProviderType);
                    cmd.CommandText = sql;

                    foreach (DataRow dataRow in SchemaDataTable.Rows)
                    {
                        string columnName = (string)dataRow[0];
                        try
                        {
                            object value = dataReader.GetValue(dataReader.GetOrdinal(columnName));

                            Type type = (Type)dataRow["DataType"];
                            if (type == typeof(SqlGeometry))
                            {
                                SqlGeometry g = (SqlGeometry)value;

                                if (g != null)
                                {
                                    //char[] chars = g.STAsText().Value;
                                    //value = "'" + new string(chars) + "'";
                                    value = g.STAsBinary().Value;
                                }
                                else
                                {
                                    value = null;
                                }
                                cmd.Parameters.Add(MySqlParameterUtils.BuildSqlParameter("?" + columnName, SqlDbType.Binary, value));
                            }
                            else if (type == typeof(SqlGeography))
                            {
                                SqlGeography geog = (SqlGeography)value;

                                if (geog != null)
                                {
                                    SqlGeometry geom = SQLSpatialToolsFunctions.VacuousGeographyToGeometry(geog, geog.STSrid.Value);
                                    //char[] chars = geom.STAsText().Value;
                                    //value = "'" + new string(chars) + "'";
                                    value = geom.STAsBinary().Value;
                                }
                                else
                                {
                                    value = null;
                                }
                                cmd.Parameters.Add(MySqlParameterUtils.BuildSqlParameter("?" + columnName, SqlDbType.Binary, value));
                            }
                            else
                            {
                                cmd.Parameters.Add(MySqlParameterUtils.BuildSqlParameter("?" + columnName, SqlDbType.VarChar, value));
                            }
                        }
                        catch (Exception e)
                        {
                            throw new Exception("error in ImportFile: " + e.Message);
                        }

                    }




                    QueryManager.AddParameters(cmd.Parameters);
                    QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, false);
                }
            }

            QueryManager.Connection.Close();

        }
    }
}
