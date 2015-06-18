using System;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.ConnectionStringManagers;

namespace USC.GISResearchLab.Common.Databases.MySql
{
    public class MySqlConnectionStringManager : AbstractConnectionStringManager
    {
        public MySqlConnectionStringManager()
        {
            DatabaseType = DatabaseType.MySql;
        }

        public MySqlConnectionStringManager(string pathToDatabaseDlls, string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
            PathToDatabaseDLLs = pathToDatabaseDlls;
        }

        public MySqlConnectionStringManager(string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
        }

        public override string GetConnectionString(DataProviderType dataProviderType)
        {
            string ret = null;
            switch (dataProviderType)
            {
                case DataProviderType.MySql:
                    ret = "Server=" + Location + ";Uid=" + UserName + ";Pwd=" + Password + ";Database=" + DefaultDatabase + ";";
                    break;
                case DataProviderType.Odbc:
                    ret = "";
                    break;
                case DataProviderType.OleDb:
                    ret = "";
                    break;
                default:
                    throw new Exception("Unexpected dataProviderType: " + dataProviderType);
            }
            return ret;
        }
    }
}
