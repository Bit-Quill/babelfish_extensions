#include "constants.h"
using std::string;

map<constants::ServerType, string> constants::server_to_odbc_types = { 
    {ServerType::MSSQL, "MSSQL"}, 
    {ServerType::PSQL, "PSQL"} 
};
