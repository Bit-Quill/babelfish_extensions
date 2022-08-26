#include <gtest/gtest.h>
#include <map>
#include <string>
#include "odbc_handler.h"

using std::map;
using std::string;

int main(int argc, char **argv) {
  OdbcHandler odbcHandler;
  map<ServerType, ConnectionStringObject> drivers = odbcHandler.getOdbcDrivers();
  string filter_string = "";

  map<ServerType, ConnectionStringObject>::iterator it;
  for (it = drivers.begin(); it != drivers.end(); it++)
  {
    std::cout<<it->second.GetConnectionString()<<"\n";
    filter_string.append(it->second.GetTestToRun());
    filter_string.append("*:");
  }
  
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = filter_string;
  return RUN_ALL_TESTS();

}