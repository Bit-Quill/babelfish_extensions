#ifndef ODBC__constants_H_
#define ODBC__constants_H_

#include <map>
#include <string>

using std::map;
using std::string;

namespace constants {

  enum class ServerType {
    MSSQL,
    PSQL,
  };

  extern map<ServerType, string> server_to_odbc_types;

  extern string MSSQL_ODBC_DRIVER_NAME;

}

#endif
