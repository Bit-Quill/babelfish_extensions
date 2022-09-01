#include "odbc_handler.h"

ConnectionStringObject::ConnectionStringObject(string driver, string server, string port, string uid, string pwd, string dbname, string test_to_run) {
    db_driver_ = driver;
    db_server_ = server;
    db_port_ = port;
    db_uid_ = uid;
    db_pwd_ = pwd;
    db_dbname_ = dbname;
    test_to_run_ = test_to_run;
    connection_string_ = "DRIVER={" + db_driver_ + "};SERVER=" + db_server_ + ";PORT=" + db_port_ + ";UID=" + db_uid_ + ";PWD=" + db_pwd_ + ";DATABASE=" + db_dbname_;
}

ConnectionStringObject::~ConnectionStringObject() {
}

string ConnectionStringObject::GetConnectionString() {
  return connection_string_;
}

string ConnectionStringObject::GetDriver() {
  return db_driver_;
}

string ConnectionStringObject::GetServer() {
  return db_server_;
}

string ConnectionStringObject::GetPort() {
  return db_port_;
}

string ConnectionStringObject::GetUid() {
  return db_uid_;
}

string ConnectionStringObject::GetPwd() {
  return db_pwd_;
}

string ConnectionStringObject::GetDbname() {
  return db_dbname_;
}

string ConnectionStringObject::GetTestToRun() {
  return test_to_run_;
}