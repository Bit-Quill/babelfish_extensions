#ifndef DRIVERS_H
#define DRIVERS_H

#include "constants.h"
#include "connection_object.h"

using namespace constants;

class Drivers {

  public:

    static map<ServerType, ConnectionObject> GetOdbcDrivers() {
      static Drivers singleton;
      return singleton.PrivGetOdbcDrivers();
    }

  private:
  
    // Constructor
    Drivers();

    // Destructor
    ~Drivers();

    void SetDrivers();

    map<ServerType, ConnectionObject> PrivGetOdbcDrivers();

    static map<ServerType, ConnectionObject> odbc_drivers_;

    // Goes through config.txt and returns a map with values from the configuration file
    map<string, string> ParseConfigFile();

    // Checks if given connection string parameters forms a valid connection object
    bool IsValidConnectionObject(string driver, string server, string port, string uid, string pwd, string dbname);
};
#endif