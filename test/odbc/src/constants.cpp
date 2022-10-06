#include "constants.h"

map<constants::ServerType, string> constants::server_to_odbc_types = { 
    {ServerType::MSSQL, "MSSQL"}, 
    {ServerType::PSQL, "PSQL"} 
};

string constants::MSSQL_ODBC_DRIVER_NAME = "ODBC Driver 17 for SQL Server";
