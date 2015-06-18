using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using MySql.Data.MySqlClient;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.ImportStatusManagers;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Databases.SchemaManagers;
using USC.GISResearchLab.Common.Databases.StoredProcedures;
using USC.GISResearchLab.Common.Diagnostics.TraceEvents;
using USC.GISResearchLab.Common.Utils.Databases;

namespace USC.GISResearchLab.Common.Databases.MySql
{
    public class MySqlImportStatusManager : AbstractImportStatusManager
    {
       

        //#region Properties


        //public TraceSource TraceSource { get; set; }
        //public string ApplicationConnectionString { get; set; }
        //public DataProviderType ApplicationDataProviderType { get; set; }
        //public DatabaseType ApplicationDatabaseType { get; set; }
        //public string ApplicationPathToDatabaseDlls { get; set; }

        private IQueryManager _QueryManager;
        public IQueryManager QueryManager
        {
            get
            {
                if (_QueryManager == null)
                {
                    _QueryManager = new QueryManager(ApplicationPathToDatabaseDlls, ApplicationDataProviderType, ApplicationDatabaseType, ApplicationConnectionString);
                }
                return _QueryManager;
            }
        }

        //private ISchemaManager _SchemaManager;
        //public ISchemaManager SchemaManager
        //{
        //    get
        //    {
        //        if (_SchemaManager == null)
        //        {
        //            _SchemaManager = new SchemaManager(ApplicationPathToDatabaseDlls, ApplicationDataProviderType, ApplicationDatabaseType, ApplicationConnectionString);
        //        }
        //        return _SchemaManager;
        //    }
        //}

        //#endregion

        public MySqlImportStatusManager(TraceSource traceSource)
        {
            TraceSource = traceSource;
        }


        public MySqlImportStatusManager(DataProviderType providerType)
        {
            ProviderType = providerType;
        }

        public MySqlImportStatusManager(DataProviderType providerType, string location, string defaultDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = providerType;
            Location = location;
            DefaultDatabase = defaultDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
        }

        public MySqlImportStatusManager(string pathToDatabaseDLLs, DataProviderType providerType, string location, string defaultDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = providerType;
            Location = location;
            DefaultDatabase = defaultDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
            PathToDatabaseDLLs = pathToDatabaseDLLs;
        }

        public MySqlImportStatusManager(string pathToDatabaseDlls, DataProviderType providerType, string connectionString)
        {
            SchemaManager = SchemaManagerFactory.GetSchemaManager(pathToDatabaseDlls, providerType, connectionString);
        }
        

        public override void InitializeConnections()
        {

        }

        public override void CreateStoredProcedures(bool shouldThrowExceptions)
        {
            try
            {
                /*
                SchemaManager.QueryManager.Connection.Open();

                ForiegnKeyRemover foriegnKeyRemover = new ForiegnKeyRemover(SchemaManager.QueryManager.Connection.Database);
                string dropForeignKeysDropSql = foriegnKeyRemover.GetDropSQL();
                string dropForeignKeysCreateSql = foriegnKeyRemover.GetCreateSQL();

                SchemaManager.AddStoredProcedureToDatabase(dropForeignKeysDropSql, false);
                SchemaManager.AddStoredProcedureToDatabase(dropForeignKeysCreateSql, false);
                 */
            }
            catch (Exception e)
            {
                string msg = "Error CreateStoredProcedures: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                if (shouldThrowExceptions)
                {
                    throw new Exception(msg, e);
                }
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override void CreateImportStatusStateTable(string tableName, bool restart)
        {
            try
            {
                SchemaManager.QueryManager.Connection.Open();
                
                if (restart)
                {
                    SchemaManager.RemoveTableFromDatabase(tableName);
                }

                string sql = "use " + SchemaManager.QueryManager.Connection.Database + "; ";
                //sql += "IF NOT EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "')";
                sql += "CREATE TABLE " + tableName + " (";
                sql += "id bigint NOT NULL AUTO_INCREMENT,";
                sql += "state varchar(255) DEFAULT NULL,";
                sql += "status varchar(255) DEFAULT NULL,";
                sql += "startDate datetime DEFAULT NULL,";
                sql += "endDate datetime DEFAULT NULL,";
                sql += "message varchar(1000) DEFAULT NULL,";
                sql += "PRIMARY KEY  (id)";
                sql += ");";


                SchemaManager.AddTableToDatabase(tableName, sql);
            }
            catch (Exception e)
            {
                string msg = "Error CreateImportStatusStateTable: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                throw new Exception(msg, e);
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override void CreateImportStatusCountyTable(string tableName, bool restart)
        {
            try
            {
                SchemaManager.QueryManager.Connection.Open();

                if (restart)
                {
                    SchemaManager.RemoveTableFromDatabase(tableName);
                }

                string sql = "use " + SchemaManager.QueryManager.Connection.Database + "; ";
                //sql += "IF NOT EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "')";
                sql += "CREATE TABLE " + tableName + " (";
                sql += "id bigint NOT NULL AUTO_INCREMENT,";
                sql += "state varchar(255) DEFAULT NULL,";
                sql += "county varchar(255) DEFAULT NULL,";
                sql += "status varchar(255) DEFAULT NULL,";
                sql += "startDate datetime DEFAULT NULL,";
                sql += "endDate datetime DEFAULT NULL,";
                sql += "message varchar(1000) DEFAULT NULL,";
                sql += "PRIMARY KEY  (id)";
                sql += ");";

                SchemaManager.AddTableToDatabase(tableName, sql);
            }
            catch (Exception e)
            {
                string msg = "Error CreateImportStatusCountyTable: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                throw new Exception(msg, e);
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override void CreateImportStatusFileTable(string tableName, bool restart)
        {
            try
            {
                SchemaManager.QueryManager.Connection.Open();

                if (restart)
                {
                    SchemaManager.RemoveTableFromDatabase(tableName);
                }

                string sql = "use " + SchemaManager.QueryManager.Connection.Database + "; ";
                //sql += "IF NOT EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "')";
                sql += "CREATE TABLE " + tableName + " (";
                sql += "id bigint NOT NULL AUTO_INCREMENT,";
                sql += "state varchar(255) DEFAULT NULL,";
                sql += "county varchar(255) DEFAULT NULL,";
                sql += "filename varchar(255) DEFAULT NULL,";
                sql += "status varchar(255) DEFAULT NULL,";
                sql += "startDate datetime DEFAULT NULL,";
                sql += "endDate datetime DEFAULT NULL,";
                sql += "message varchar(1000) DEFAULT NULL,";
                sql += "PRIMARY KEY  (id)";
                sql += ");";

                SchemaManager.AddTableToDatabase(tableName, sql, false);
            }
            catch (Exception e)
            {
                string msg = "Error CreateImportStatusFileTable: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                throw new Exception(msg, e);
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override bool CheckStatusStateAlreadyDone(string tableName, string state)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "checking state status: " + state);
                }

                string sql = "select id FROM " + tableName + "";
                sql += " where ";
                sql += " state=?state";
                sql += " and ";
                sql += " status='Finished'";

                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                if (id > 0)
                {
                    ret = true;
                }

            }
            catch (Exception exc)
            {
                string msg = "Error checking state status: " + state;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool CheckStatusCountyAlreadyDone(string tableName, string county)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "checking county status: " + county);
                }

                string sql = "select id FROM " + tableName + "";
                sql += " where ";
                sql += " county=?county";
                sql += " and ";
                sql += " status='Finished'";

                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                if (id > 0)
                {
                    ret = true;
                }

            }
            catch (Exception exc)
            {
                string msg = "Error checking county status: " + county;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool CheckStatusFileAlreadyDone(string tableName, string state, string county, string file)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "checking file status: " + file);
                }

                string sql = "select id FROM " + tableName + "";
                sql += " where ";
                sql += " filename=?filename";
                sql += " and ";
                sql += " state=?state";
                sql += " and ";
                sql += " county=?county";
                sql += " and ";
                sql += " status='Finished'";

                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?filename", SqlDbType.VarChar, file));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                if (id > 0)
                {
                    ret = true;
                }

            }
            catch (Exception exc)
            {
                string msg = "Error checking file status: " + file;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        

        public override bool UpdateStatusFile(string tableName, string state, string county, string file, Statuses status, string message)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "updating file status: " + file + " status: " + status);
                }

                string sql = "select id FROM " + tableName + "";
                sql += " where ";
                sql += " filename=?filename";
                sql += " and ";
                sql += " county=?county";
                sql += " and ";
                sql += " state=?state";

                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?filename", SqlDbType.VarChar, file));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                if (id <= 0 || status == Statuses.start)
                {
                    InsertStatusFile(tableName, state, county,  file);
                }
                else
                {
                    sql = "update " + tableName;
                    sql += " set ";
                    sql += " status=?status,";
                    sql += " endDate=?endDate,";
                    sql += " message=?message";
                    sql += " where ";
                    sql += " id=?id ";

                    cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, GetStatusString(status)));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?endDate", SqlDbType.DateTime, DateTime.Now));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?message", SqlDbType.VarChar, message));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?id", SqlDbType.BigInt, id));

                    SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                    SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);
                }


                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error updating file status: " + file + " status: " + status;
                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }
                throw new Exception(msg, exc);
            }
            return ret;
        }

        

        public override bool UpdateStatusState(string tableName, string state, Statuses status, string message)
        {
            bool ret = false;

            try
            {
                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "updating state status: " + state + " status: " + status);
                }

                string sql = "select id FROM " + tableName + "";
                sql += " where ";
                sql += " state=?state";

                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                if (id <= 0 || status == Statuses.start)
                {
                    InsertStatusState(tableName, state);
                }
                else
                {
                    sql = "update " + tableName + "";
                    sql += " set ";
                    sql += " status=?status,";
                    sql += " endDate=?endDate,";
                    sql += " message=?message";
                    sql += " where ";
                    sql += " id=?id ";

                    cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, GetStatusString(status)));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?endDate", SqlDbType.DateTime, DateTime.Now));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?message", SqlDbType.VarChar, message));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?id", SqlDbType.BigInt, id));

                    SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                    SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);
                }

                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error updating state status: " + state + " status: " + status + ":" + exc.Message;
                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }
                throw new Exception(msg, exc);
            }
            return ret;
        }

       

        public override bool UpdateStatusCounty(string tableName, string state, string county, Statuses status, string message)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "updating county status: " + county + " status: " + status);
                }

                string sql = "select id FROM " + tableName + "";
                sql += " where ";
                sql += " state=?state";
                sql += " and ";
                sql += " county=?county";

                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                

                if (id <= 0 || status == Statuses.start)
                {
                    InsertStatusCounty(tableName, state, county);
                }
                else
                {
                    sql = "update " + tableName + "";
                    sql += " set ";
                    sql += " status=?status,";
                    sql += " endDate=?endDate,";
                    sql += " message=?message";
                    sql += " where ";
                    sql += " id=?id ";

                    cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, GetStatusString(status)));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?endDate", SqlDbType.DateTime, DateTime.Now));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?message", SqlDbType.VarChar, message));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?id", SqlDbType.BigInt, id));

                    SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                    SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);
                }

                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error updating county status: " + county + " status: " + status + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool InsertStatusFile(string tableName, string state, string county, string file)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "inserting file status: " + file + " status: ");
                }

                string sql = "INSERT into " + tableName + "";
                sql += " (";
                sql += " state,";
                sql += " county,";
                sql += " filename,";
                sql += " status,";
                sql += " startDate";
                sql += " )";
                sql += " VALUES ";
                sql += " (";
                sql += " ?state,";
                sql += " ?county,";
                sql += " ?filename,";
                sql += " ?status,";
                sql += " ?startDate";
                sql += " )";


                IDbCommand cmd = new MySqlCommand(sql);
                cmd.Parameters.Add(MySqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(MySqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));
                cmd.Parameters.Add(MySqlParameterUtils.BuildSqlParameter("?filename", SqlDbType.VarChar, file));
                cmd.Parameters.Add(MySqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, "Started"));
                cmd.Parameters.Add(MySqlParameterUtils.BuildSqlParameter("?startDate", SqlDbType.DateTime, DateTime.Now));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);

                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error inserting file status: " + file + " status: " + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool InsertStatusState(string tableName, string state)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "inserting state status: " + state + " status: ");
                }

                string sql = "INSERT into " + tableName + "";
                sql += " (";
                sql += " state,";
                sql += " status,";
                sql += " startDate";
                sql += " )";
                sql += " VALUES ";
                sql += " (";
                sql += " ?state,";
                sql += " ?status,";
                sql += " ?startDate";
                sql += " )";


                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, "Started"));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?startDate", SqlDbType.DateTime, DateTime.Now));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);


                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error inserting state status: " + state + " status: " + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool InsertStatusCounty(string tableName, string state, string county)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "inserting county status: " + county + " status: ");
                }

                string sql = "INSERT into " + tableName + "";
                sql += " (";
                sql += " state,";
                sql += " county,";
                sql += " status,";
                sql += " startDate";
                sql += " )";
                sql += " VALUES ";
                sql += " (";
                sql += " ?state,";
                sql += " ?county,";
                sql += " ?status,";
                sql += " ?startDate";
                sql += " )";


                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, "Started"));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?startDate", SqlDbType.DateTime, DateTime.Now));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);


                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error inserting county status: " + county + " status: " + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }
    }
}
