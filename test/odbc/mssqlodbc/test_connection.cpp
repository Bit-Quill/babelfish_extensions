#include "../odbc_handler.h"
#include <gtest/gtest.h>
#include <sql.h>
#include <sqlext.h>
#include <iostream>
#include <string>

using std::unique_ptr;

class MSSQL_Connection : public testing::Test{

};

TEST_F(MSSQL_Connection, SQLDriverConnect_1_SuccessfulConnectionTest) {

  OdbcHandler odbcHandler;

  odbcHandler.AllocateEnvironmentHandle();
  odbcHandler.AllocateConnectionHandle();

  RETCODE rcode = SQLDriverConnect(odbcHandler.GetConnectionHandle(),
                                nullptr, 
                                (SQLCHAR *) odbcHandler.GetConnectionString(ServerType::MSSQL).c_str(), 
                                SQL_NTS, 
                                nullptr, 
                                0, 
                                nullptr, 
                                SQL_DRIVER_COMPLETE);

  ASSERT_TRUE(rcode == SQL_SUCCESS_WITH_INFO || rcode == SQL_SUCCESS);
}
