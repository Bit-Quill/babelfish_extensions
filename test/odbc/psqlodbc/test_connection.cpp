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
  // SQLHENV env;
  //   SQLHDBC dbc;
  //   SQLHSTMT stmt;
  //   SQLRETURN rc;

  //   //*** Connecting to remote PostgreSQL database
  //   rc = SQLAllocEnv( &env );
  //   rc = SQLAllocConnect( env, &dbc );
  //   if( SQL_SUCCEEDED(rc) )
  //   {
  //       std::cout << "allocation successful" << std::endl;
  //   }
  //   std::string _szDSN = "DRIVER={PostgreSQL ODBC Driver(UNICODE)};Server=localhost;Port=5432;Database=jdbc_testdb;UID=jdbc_user;PWD=12345678";
  //   rc = SQLDriverConnect( dbc, NULL, (unsigned char*)(_szDSN.c_str()), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT );
  //   if( SQL_SUCCEEDED(rc) )
  //   {
  //       std::cout << "Connection successful" << std::endl;
  //   }
  //   else
  //   {
  //       std::cout << "Connection UNsuccessful" << std::endl;
  //   }


  //   //*** Exit program
  //   std::cout.flush();
  //   std::getchar();
  //   return;
}
