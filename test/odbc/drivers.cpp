#include "drivers.h"
#include <fstream>

using std::pair;

Drivers::Drivers() {
  if (odbc_drivers_.empty()) {
   SetDrivers();
  }
}

Drivers::~Drivers() {
}

void Drivers::SetDrivers() {

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
      odbc_drivers_.insert(pair<ServerType, ConnectionStringObject>(it->first,cso));
  }
  return; 

}

map<string, string> Drivers::ParseConfigFile() {

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

map<ServerType, ConnectionStringObject> Drivers::GetOdbcDrivers() {
  return odbc_drivers_;
}

map<ServerType, ConnectionStringObject> Drivers::odbc_drivers_{};