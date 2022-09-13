#include "connection_object.h"
#include "odbc_handler.h"

ConnectionObject::ConnectionObject(string driver, string server, string port, string uid, string pwd, string dbname) {
    db_driver_ = driver;
    db_server_ = server;
    db_port_ = port;
    db_uid_ = uid;
    db_pwd_ = pwd;
    db_dbname_ = dbname;
    
    connection_string_ = "DRIVER={" + db_driver_ + "};SERVER=" + db_server_ + ";PORT=" + db_port_ + ";UID=" + db_uid_ + ";PWD=" + db_pwd_ + ";DATABASE=" + db_dbname_;
}

ConnectionObject::~ConnectionObject() {
}

string ConnectionObject::GetConnectionString() {
  return connection_string_;
}

string ConnectionObject::GetDriver() {
  return db_driver_;
}

string ConnectionObject::GetServer() {
  return db_server_;
}

string ConnectionObject::GetPort() {
  return db_port_;
}

string ConnectionObject::GetUid() {
  return db_uid_;
}

string ConnectionObject::GetPwd() {
  return db_pwd_;
}

string ConnectionObject::GetDbname() {
  return db_dbname_;
}
