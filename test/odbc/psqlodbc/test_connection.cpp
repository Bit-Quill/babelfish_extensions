#include "../odbc_handler.h"
#include <gtest/gtest.h>
#include <sql.h>
#include <sqlext.h>
#include <iostream>
#include <string>

using std::unique_ptr;

class PSQL_Connection : public testing::Test{

};

TEST_F(PSQL_Connection, SQLDriverConnect_2_SuccessfulConnectionTest) {

  OdbcHandler odbcHandler;

  odbcHandler.AllocateEnvironmentHandle();
  odbcHandler.AllocateConnectionHandle();

  RETCODE rcode = SQLDriverConnect(odbcHandler.GetConnectionHandle(),
                                nullptr, 
                                (SQLCHAR *) odbcHandler.GetConnectionString(ServerType::PSQL).c_str(), 
                                SQL_NTS, 
                                nullptr, 
                                0, 
                                nullptr, 
                                SQL_DRIVER_COMPLETE);

  ASSERT_TRUE(rcode == SQL_SUCCESS_WITH_INFO || rcode == SQL_SUCCESS);
}
