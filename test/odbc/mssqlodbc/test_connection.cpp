#include "../odbc_handler.h"
#include <gtest/gtest.h>
#include <sqlext.h>
#include "../drivers.h"

class MSSQL_Connection : public testing::Test{
  void SetUp() override {
    map<constants::ServerType, ConnectionObject> available_drivers = Drivers::GetOdbcDrivers();
    if (available_drivers.find(ServerType::MSSQL) == available_drivers.end())
      GTEST_SKIP() << "MSSQL Driver not present: skipping all tests for this fixture.";
  }

};

TEST_F(MSSQL_Connection, SQLDriverConnect_SuccessfulConnectionTest) {

  OdbcHandler odbcHandler(Drivers::GetOdbcDrivers().at(ServerType::MSSQL));

  odbcHandler.AllocateEnvironmentHandle();
  odbcHandler.AllocateConnectionHandle();

  RETCODE rcode = SQLDriverConnect(odbcHandler.GetConnectionHandle(),
                                nullptr, 
                                (SQLCHAR *) odbcHandler.GetConnectionString().c_str(), 
                                SQL_NTS, 
                                nullptr, 
                                0, 
                                nullptr, 
                                SQL_DRIVER_COMPLETE);

  ASSERT_TRUE(rcode == SQL_SUCCESS_WITH_INFO || rcode == SQL_SUCCESS);
}
