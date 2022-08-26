
#include "odbc_handler.h"
#include <sqlext.h>
#include <fstream>
#include <gtest/gtest.h>

using std::pair;

OdbcHandler::OdbcHandler() {
  SetConnectionString();
}

OdbcHandler::~OdbcHandler() {
  FreeStmtHandle();
  FreeConnectionHandle();
  FreeEnvironmentHandle();
}

void OdbcHandler::Connect(bool allocate_statement_handle, ServerType st) {

  const int MAX_ATTEMPTS = 4;
  int attempts = 0;
  string connection_str = GetConnectionString(st);
  AllocateEnvironmentHandle();
  AllocateConnectionHandle();
  SQLSetConnectAttr(hdbc_, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)5, 0); // 5 seconds
  
  do {
    attempts++;
    retcode_ = SQLDriverConnect(hdbc_, nullptr, (SQLCHAR *) connection_str.c_str(), SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_COMPLETE);
  } while (retcode_ != SQL_SUCCESS && retcode_ != SQL_SUCCESS_WITH_INFO && attempts < MAX_ATTEMPTS );
  
  AssertSqlSuccess(retcode_, "CONNECTION FAILED");

  if (allocate_statement_handle) {
    AllocateStmtHandle();
  }
}

SQLHENV OdbcHandler::GetEnvironmentHandle() {
  return this->henv_;
}

SQLHDBC OdbcHandler::GetConnectionHandle() {
  return this->hdbc_;
}

SQLHSTMT OdbcHandler::GetStatementHandle() {
  return this->hstmt_;
}

RETCODE OdbcHandler::GetReturnCode() {
  return this->retcode_;
}

map<ServerType, ConnectionStringObject> OdbcHandler::getOdbcDrivers() {
  return this->odbc_drivers;
}

string OdbcHandler::GetConnectionString(ServerType st) {
  map<ServerType, ConnectionStringObject> drivers_map = this->odbc_drivers;
  ConnectionStringObject cso = drivers_map.find(st)->second;
  string connection_str = cso.GetConnectionString();
  return connection_str;
}

void OdbcHandler::AllocateEnvironmentHandle() {

  if (henv_ != NULL) {
    FAIL() << "ERROR: There was an attempt to allocate an already allocated environment handle";
  }

  AssertSqlSuccess(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv_),
                "ERROR: Failed to allocate the environment handle");

  SQLSetEnvAttr(henv_,
                SQL_ATTR_ODBC_VERSION,
                (SQLPOINTER)SQL_OV_ODBC3,
                0);
}

void OdbcHandler::AllocateConnectionHandle() {

  if (hdbc_ != NULL) {
    FAIL() << "ERROR: There was an attempt to allocate an already allocated connection handle";
  }

  AssertSqlSuccess(
    SQLAllocHandle(SQL_HANDLE_DBC, henv_, &hdbc_),
    "ERROR: Failed to allocate connection handle");
  
}

void OdbcHandler::AllocateStmtHandle() {
  if (hstmt_ != NULL) {
    FAIL() << "ERROR: There was an attempt to allocate an already allocated statement handle";
  }

  AssertSqlSuccess( 
    SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt_), 
    "ERROR: Failed to allocate connection handle");
}

void OdbcHandler::FreeEnvironmentHandle() {
  if (henv_) {
    SQLFreeHandle(SQL_HANDLE_ENV, henv_);
    henv_ = NULL;
  }
}

void OdbcHandler::FreeConnectionHandle() {
  if (hdbc_) {
    SQLDisconnect(hdbc_);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc_);
    hdbc_ = NULL;
  }
}

void OdbcHandler::FreeStmtHandle() {
  if (hstmt_) {
    SQLFreeStmt(hstmt_, SQL_CLOSE);
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt_);
    hstmt_ = NULL;
  }
}

void OdbcHandler::FreeAllHandles() {
  FreeStmtHandle();
  FreeConnectionHandle();
  FreeEnvironmentHandle();
}

void OdbcHandler::CloseStmt() {
  if (hstmt_) {
    SQLFreeStmt(hstmt_, SQL_CLOSE);
  }
}

void OdbcHandler::SetConnectionString () {

  map<string, string> config_file_values = ParseConfigFile();

  static map<ServerType, string>::iterator it;
  for (it = server_to_odbc_types.begin(); it != server_to_odbc_types.end(); it++)
  {
    string env_db_driver_ = it->second + "_ODBC_DRIVER_NAME";
    string env_db_server_ = it->second + "_BABEL_DB_SERVER";
    string env_db_port_ = it->second + "_BABEL_DB_PORT";
    string env_db_uid_ = it->second + "_BABEL_DB_USER";
    string env_db_pwd_ = it->second + "_BABEL_DB_PASSWORD";
    string env_db_dbname_ = it->second + "_BABEL_DB_NAME";
    string env_test_to_run_ = it->second + "_TEST_TO_RUN";

    string db_driver_ = getenv(env_db_driver_.c_str()) ? string(getenv(env_db_driver_.c_str())) : 
        config_file_values.find(env_db_driver_) != config_file_values.end() ? config_file_values[env_db_driver_] : "";
        
    string db_server_ = getenv(env_db_server_.c_str()) ? string(getenv(env_db_server_.c_str())) :
        config_file_values.find(env_db_server_) != config_file_values.end() ? config_file_values[env_db_server_] : "";

    string db_port_ = getenv(env_db_port_.c_str()) ? string(getenv(env_db_port_.c_str())) :
        config_file_values.find(env_db_port_) != config_file_values.end() ? config_file_values[env_db_port_] : "";

    string db_uid_ = getenv(env_db_uid_.c_str()) ? string(getenv(env_db_uid_.c_str())) :
        config_file_values.find(env_db_uid_) != config_file_values.end() ? config_file_values[env_db_uid_] : "";

    string db_pwd_ = getenv(env_db_pwd_.c_str()) ? string(getenv(env_db_pwd_.c_str())) :
        config_file_values.find(env_db_pwd_) != config_file_values.end() ? config_file_values[env_db_pwd_] : "";

    string db_dbname_ = getenv(env_db_dbname_.c_str()) ? string(getenv(env_db_dbname_.c_str())) :
        config_file_values.find(env_db_dbname_) != config_file_values.end() ? config_file_values[env_db_dbname_] : "";
    
    string test_to_run_ = getenv(env_test_to_run_.c_str()) ? string(getenv(env_test_to_run_.c_str())) :
        config_file_values.find(env_test_to_run_) != config_file_values.end() ? config_file_values[env_test_to_run_] : "";
    
    ConnectionStringObject cso(db_driver_, db_server_, db_port_, db_uid_, db_pwd_, db_dbname_, test_to_run_);
    if (test_to_run_ != "")
      odbc_drivers.insert(pair<ServerType, ConnectionStringObject>(it->first,cso));
  }
  return; 
}

void OdbcHandler::AssertSqlSuccess(RETCODE retcode, const string& error_msg) {
  if (!IsSqlSuccess(retcode)) {
    FAIL() << error_msg << std::endl << "Return code was: " << retcode << "\n" << "SQL Status of: " << GetSqlState(SQL_HANDLE_DBC, hdbc_); 
  }
}

bool OdbcHandler::IsSqlSuccess(RETCODE retcode) {
  return retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO;
}

map<string, string> OdbcHandler::ParseConfigFile() {

    string line{};
    map<string, string> config_file_values{};
    std::ifstream config_file;
    config_file.open("config.txt");

    if (!config_file.is_open()) {
        // ERROR: Cannot open config file
        return config_file_values;
    }

    while (std::getline(config_file, line)) {

      size_t index = line.find("=");

      if (index == string::npos || index == (line.length() - 1)) {
      // an empty line
        continue;
      }

      string key = line.substr(0,index);
      string value = line.substr(index+1);

      if (value.find_first_not_of(' ') == string::npos) {
        // value consists of only empty spaces
        continue;
      }
      config_file_values.insert(pair<string, string>(key,value));

    }
    return config_file_values;
}

void OdbcHandler::ExecQuery(const string& query) {
  
  AssertSqlSuccess(
      SQLExecDirect(hstmt_, (SQLCHAR*) query.c_str(), SQL_NTS),
      "ERROR: was not able to run query: " + query);

}

void OdbcHandler::ConnectAndExecQuery(const string& query){

  this->Connect(true);
  this->ExecQuery(query);
}


string OdbcHandler::GetSqlState(SQLSMALLINT HandleType, SQLHANDLE Handle) {

  SQLINTEGER native_error_ptr; 
  SQLCHAR sql_error_msg[1024];
  SQLCHAR sql_state[SQL_SQLSTATE_SIZE+1];

  SQLGetDiagRec(HandleType,
                       Handle,
                       1,
                       sql_state,
                       &native_error_ptr,
                       sql_error_msg,
                       (SQLSMALLINT)(sizeof(sql_error_msg) / sizeof(SQLCHAR)),
                       nullptr);
  return string(reinterpret_cast<char *>(sql_state));
}

string OdbcHandler::GetErrorMessage(SQLSMALLINT HandleType, const RETCODE& retcode) {

  SQLINTEGER native_error_ptr;
  SQLCHAR sql_error_msg[1024] = "(Unable to retrieve error message)";
  SQLCHAR sql_state[SQL_SQLSTATE_SIZE+1] = "";
  SQLHANDLE handle;

  if (retcode == SQL_SUCCESS) {
    return "SQL_SUCCESS was returned but was not expected";
  }

  switch(HandleType) {
    case SQL_HANDLE_ENV:
      handle = henv_;
      break;
    case SQL_HANDLE_DBC:
      handle = hdbc_;
      break;
    case SQL_HANDLE_STMT:
      handle = hstmt_;
      break;
    default:
      return "(Unable to retrieve error message - an invalid handle type was passed. Please ensure you are passing SQL_HANDLE_ENV, SQL_HANDLE_DBC, or SQL_HANDLE_STMT";
  }

  SQLGetDiagRec(HandleType,
                       handle,
                       1,
                       sql_state,
                       &native_error_ptr,
                       sql_error_msg,
                       (SQLSMALLINT)(sizeof(sql_error_msg) / sizeof(SQLCHAR)),
                       nullptr);
  return "[Return value: " + std::to_string(retcode) + "][SQLState: " + std::string((const char *) sql_state) + "] ERROR: " + std::string((const char*) sql_error_msg) + "\n";
}

void OdbcHandler::BindColumns(vector<tuple<int, int, SQLPOINTER, int>> columns) {
  RETCODE rcode;

  for (auto column : columns) {
    auto& [col_num, c_type, target, target_size] = column;
    rcode = SQLBindCol(GetStatementHandle(), col_num, c_type, target, target_size, 0);
    ASSERT_EQ(rcode, SQL_SUCCESS) << GetErrorMessage(SQL_HANDLE_STMT, rcode);
  }
}
