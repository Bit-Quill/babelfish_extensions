#ifndef CONNECTION_STRING_OBJECT_H
#define CONNECTION_STRING_OBJECT_H

#include "odbc_handler.h"
#include <string>

using std::string;
using namespace constants;


class ConnectionStringObject {

  public:

  ConnectionStringObject(string driver, string server, string port, string uid, string pwd, string dbname, string test_to_run);
  ~ConnectionStringObject();

  // Returns the connection string 
  string GetConnectionString();

  // Returns the driver name
  string GetDriver();

  // Returns the server name
  string GetServer();

  // Returns the port
  string GetPort();

  // Returns the username/uid used for database login
  string GetUid();

  // Returns the password used for database login
  string GetPwd();

  // Returns the database used
  string GetDbname();

  // Returns prefix of test suites to run for this particular ODBC driver
  string GetTestToRun();

  private:
    
    // DB Information
    string db_driver_;
    string db_server_;
    string db_port_;
    string db_uid_;
    string db_pwd_;
    string db_dbname_;
    string test_to_run_;

    string connection_string_;
};

#endif

