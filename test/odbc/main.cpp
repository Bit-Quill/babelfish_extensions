#include <gtest/gtest.h>
#include "drivers.h"

int main(int argc, char **argv) {
  Drivers d;
  map<ServerType, ConnectionStringObject> drivers = d.GetOdbcDrivers();
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