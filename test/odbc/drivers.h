#ifndef DRIVERS_H
#define DRIVERS_H

#include "constants.h"
#include "connection_string_object.h"

using namespace constants;

class Drivers {

  public:

    // Constructor
    Drivers();

    // Destructor
    ~Drivers();

    void SetDrivers();

    map<ServerType, ConnectionStringObject> GetOdbcDrivers();


  private:

    static map<ServerType, ConnectionStringObject> odbc_drivers_;

    // Goes through config.txt and returns a map with values from the configuration file
    map<string, string> ParseConfigFile();
};
#endif