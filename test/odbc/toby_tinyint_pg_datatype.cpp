#include <gtest/gtest.h>
#include <sqlext.h>
#include "odbc_handler.h"
#include "query_generator.h"
#include <iostream>
#include <math.h>
using std::pair;

const string TABLE_NAME = "master_dbo.tinyint_table_odbc_test";
const string COL1_NAME = "pk";
const string COL2_NAME = "data";
const string DATATYPE_NAME = "sys.tinyint";
const string VIEW_NAME = "master_dbo.tinyint_view_odbc_test";


vector<pair<string, string>> TABLE_COLUMNS_TINYINT = {
    {COL1_NAME, DATATYPE_NAME + " PRIMARY KEY"},
    {COL2_NAME, DATATYPE_NAME}
  };

class PSQL_DataTypes_Tinyint : public testing::Test{

  void SetUp() override {
    OdbcHandler test_setup;
    test_setup.ConnectAndExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
  }

  void TearDown() override {

    OdbcHandler test_teardown;
    test_teardown.ConnectAndExecQuery(DropObjectStatement("VIEW", VIEW_NAME));
    test_teardown.CloseStmt();
    test_teardown.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
  }
};

// helper function to convert string to equivalent C version of int (long int)
long int StringToTinyInt(const string &value) {
  return strtol(value.c_str(), NULL, 10);
}

TEST_F(PSQL_DataTypes_Tinyint, Table_Creation) {

  const int LENGTH_EXPECTED = 6;
  const int PRECISION_EXPECTED = 0;
  const int SCALE_EXPECTED = 0;
  const string NAME_EXPECTED = "NULL";
  const int BYTES_EXPECTED = 2;

  const int BUFFER_SIZE = 256;
  char name[BUFFER_SIZE];
  SQLLEN length;
  SQLLEN precision;
  SQLLEN scale;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  // Create a table with columns defined with the specific datatype being tested. 
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_TINYINT));
  odbcHandler.CloseStmt();

  // Select * From Table to ensure that it exists
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL1_NAME}));

  // Make sure column attributes are correct
  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_DISPLAY_SIZE, // Get the length of the column (size of char in columns)
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &length);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(length, LENGTH_EXPECTED);    //refer to babelfish_extensions/contrib/babelfishpg_common/sql/numerics.sql
  //                                        the tinyint created based on smallint type since postgresql does not have
  //                                        tiny int datatype

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            2,
                            SQL_DESC_LENGTH, // Get the Bytes of the column 
                            NULL,
                            0,
                            NULL,
                            (SQLLEN*) &length);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(length, BYTES_EXPECTED);    //Bytes same as length problem above

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_PRECISION, // Get the precision of the column
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &precision); 
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(precision, PRECISION_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_SCALE, // Get the scale of the column
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &scale); 
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(scale, SCALE_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_TYPE_NAME, // Get the type name of the column
                          name,
                          BUFFER_SIZE,
                          NULL,
                          NULL); 
  // ASSERT_EQ(string(name), NAME_EXPECTED);  Same as problem above

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Tinyint, Insertion_Success) {

  const int BYTES_EXPECTED = 1;

  unsigned char pk;
  unsigned char data;
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector <string> valid_inserted_values = {
    "0",
    "255",
    "3",
    "NULL"
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {
    {1, SQL_C_STINYINT, &pk, 0, &pk_len},
    {2, SQL_C_STINYINT, &data, 0,  &data_len}
  };

  string insert_string{}; 
  string comma{};
  
  for (int i = 0; i< valid_inserted_values.size(); ++i) {
    insert_string += comma + "(" + std::to_string(i) + "," + valid_inserted_values[i] + ")";
    comma = ",";
  }

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_TINYINT));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));
 
  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, valid_inserted_values.size());
  
  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL1_NAME}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < valid_inserted_values.size(); ++i) {
    
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, BYTES_EXPECTED);
    ASSERT_EQ(pk, i);

    if (valid_inserted_values[i] != "NULL")
    {
      ASSERT_EQ(data_len, BYTES_EXPECTED);
      ASSERT_EQ(data, StringToTinyInt(valid_inserted_values[i]));
    }
    else 
      ASSERT_EQ(data_len, SQL_NULL_DATA);
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Tinyint, Insertion_Fail) {

  unsigned char pk;
  unsigned char data;
  SQLLEN pk_len;
  SQLLEN data_len;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector <string> invalid_inserted_values = {
    "-1",
    "256"
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {
    {1, SQL_C_STINYINT, &pk, 0, &pk_len},
    {2, SQL_C_STINYINT, &data, 0,  &data_len}
  };

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_TINYINT));
  odbcHandler.CloseStmt();

  // Attempt to insert values that are out of range and assert that they all fail
  for (int i = 0; i < invalid_inserted_values.size(); i++) {

    string insert_string = "(" + std::to_string(i) + "," + invalid_inserted_values[i] + ")";

    rcode = SQLExecDirect(odbcHandler.GetStatementHandle(), (SQLCHAR*) InsertStatement(TABLE_NAME, insert_string).c_str(), SQL_NTS);
    ASSERT_EQ(rcode, SQL_ERROR);
  }

  // Select all from the tables and assert that nothing was inserted
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL1_NAME}));
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Tinyint, Update_Success) {

  const string PK_INSERTED = "1";
  const string DATA_INSERTED = "1";
  const int NUM_UPDATES = 3;

  const string DATA_UPDATED_VALUES[NUM_UPDATES] = {
    "5",
    "0",
    "255"
  };

  const string INSERT_STRING = "(" + PK_INSERTED + "," + DATA_INSERTED + ")";
  const string UPDATE_WHERE_CLAUSE = COL1_NAME + " = " + PK_INSERTED;

  const int BYTES_EXPECTED = 1;
  const int AFFECTED_ROWS_EXPECTED =1;

  unsigned char pk;
  unsigned char data;
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {
    {1, SQL_C_STINYINT, &pk, 0, &pk_len},
    {2, SQL_C_STINYINT, &data, 0,  &data_len}
  };
  
  vector<pair<string, string>> update_col{};

  for (int i = 0; i < NUM_UPDATES; i++) {
    update_col.push_back(pair<string, string>(COL2_NAME, DATA_UPDATED_VALUES[i]));
  }

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_TINYINT));
  odbcHandler.CloseStmt();

  // Insert valid values into the table using the correct ODBC data type mapping.
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, INSERT_STRING));
  odbcHandler.CloseStmt();

  // Bind Columns
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  // Assert that value is inserted properly
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL1_NAME}));
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(pk_len, BYTES_EXPECTED);
  ASSERT_EQ(pk, StringToTinyInt(PK_INSERTED));
  ASSERT_EQ(data_len, BYTES_EXPECTED);
  ASSERT_EQ(data, StringToTinyInt(DATA_INSERTED));

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);
  odbcHandler.CloseStmt();

  // Update value multiple times
  for (int i = 0; i < NUM_UPDATES; i++) {

    odbcHandler.ExecQuery(UpdateTableStatement(TABLE_NAME, vector<pair<string,string>>{update_col[i]}, UPDATE_WHERE_CLAUSE));

    rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(affected_rows, AFFECTED_ROWS_EXPECTED);

    odbcHandler.CloseStmt();

    // Assert that updated value is present
    odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL1_NAME}));
    rcode = SQLFetch(odbcHandler.GetStatementHandle());

    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, BYTES_EXPECTED);
    ASSERT_EQ(pk, StringToTinyInt(PK_INSERTED));
    ASSERT_EQ(data_len, BYTES_EXPECTED);
    ASSERT_EQ(data, StringToTinyInt(DATA_UPDATED_VALUES[i]));

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_NO_DATA);
    odbcHandler.CloseStmt();
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Tinyint, Update_Fail) {

  const string PK_INSERTED = "1";
  const string DATA_INSERTED = "1";
  const string DATA_UPDATED_VALUE = "256"                                                      ;

  const string INSERT_STRING = "(" + PK_INSERTED + "," + DATA_INSERTED + ")";
  const string UPDATE_WHERE_CLAUSE = COL1_NAME + " = " + PK_INSERTED;

  const int BYTES_EXPECTED = 1;

  unsigned char pk;
  unsigned char data;
  SQLLEN pk_len;
  SQLLEN data_len;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {
    {1, SQL_C_STINYINT, &pk, 0, &pk_len},
    {2, SQL_C_STINYINT, &data, 0,  &data_len}
  };
  
  vector<pair<string, string>> update_col = {
    {COL2_NAME, DATA_UPDATED_VALUE}
  };

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_TINYINT));
  odbcHandler.CloseStmt();

  // Insert valid values into the table using the correct ODBC data type mapping.
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, INSERT_STRING));
  odbcHandler.CloseStmt();

  // Bind Columns
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  // Assert that value is inserted properly
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL1_NAME}));
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(pk_len, BYTES_EXPECTED);
  ASSERT_EQ(pk, StringToTinyInt(PK_INSERTED));
  ASSERT_EQ(data_len, BYTES_EXPECTED);
  ASSERT_EQ(data, StringToTinyInt(DATA_INSERTED));

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);
  odbcHandler.CloseStmt();

  // Update value and assert an error is present
  rcode = SQLExecDirect(odbcHandler.GetStatementHandle(), (SQLCHAR*) UpdateTableStatement(TABLE_NAME, update_col, UPDATE_WHERE_CLAUSE).c_str(), SQL_NTS);
  ASSERT_EQ(rcode, SQL_ERROR);
  odbcHandler.CloseStmt();

  // Assert that no values changed
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL1_NAME}));
  rcode = SQLFetch(odbcHandler.GetStatementHandle());

  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(pk_len, BYTES_EXPECTED);
  ASSERT_EQ(pk, StringToTinyInt(PK_INSERTED));
  ASSERT_EQ(data_len, BYTES_EXPECTED);
  ASSERT_EQ(data, StringToTinyInt(DATA_INSERTED));

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

// TEST_F(PSQL_DataTypes_Tinyint, DISABLED_Arithmetic_Operators) {

//   const int BYTES_EXPECTED = 1;

//   unsigned char pk;
//   unsigned char data;
//   SQLLEN pk_len;
//   SQLLEN data_len;
//   SQLLEN affected_rows;

//   RETCODE rcode;
//   OdbcHandler odbcHandler;

//   vector <string> inserted_pk = {
//     "20",
//     "30"
//   };

//   vector <string> inserted_data = {
//     "40",
//     "20"
//   };

//   vector <string> operations_query = {
//     COL1_NAME + "+" + COL2_NAME,
//     COL1_NAME + "-" + COL2_NAME,
//     COL1_NAME + "*" + COL2_NAME,
//     COL1_NAME + "/" + COL2_NAME
//   };

//   vector<vector<unsigned char>>expected_results = {{},{}};

//   // initialization of expected_results
//   for (int i = 0; i < inserted_pk.size(); i++) {
//     expected_results[i].push_back(StringToTinyInt(inserted_pk[i]) + StringToTinyInt(inserted_data[i]));
//     expected_results[i].push_back(StringToTinyInt(inserted_pk[i]) - StringToTinyInt(inserted_data[i]));
//     expected_results[i].push_back(StringToTinyInt(inserted_pk[i]) * StringToTinyInt(inserted_data[i]));
//     expected_results[i].push_back(StringToTinyInt(inserted_pk[i]) / StringToTinyInt(inserted_data[i]));
//   }

//   unsigned char col_results[operations_query.size()];
//   SQLLEN col_len[operations_query.size()];
//   vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {};

//   // initialization for bind_columns
//   for (int i = 0; i < operations_query.size(); i++) {
//     tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_STINYINT, (SQLPOINTER) &col_results[i], 0, &col_len[i]);
//     bind_columns.push_back(tuple_to_insert);
//   }

//   string insert_string{}; 
//   string comma{};
  
//   // insert_string initialization
//   for (int i = 0; i< inserted_pk.size() ; ++i) {
//     insert_string += comma + "(" + inserted_pk[i] + "," + inserted_data[i] + ")";
//     comma = ",";
//   }

//   // Create table
//   odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_TINYINT));
//   odbcHandler.CloseStmt();

//   // Insert valid values into the table and assert affected rows
//   odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));
 
//   rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
//   ASSERT_EQ(rcode, SQL_SUCCESS);
//   ASSERT_EQ(affected_rows, inserted_data.size());
  

//   // Make sure inserted values are correct and operations
//   ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

//   for (int i = 0; i < inserted_data.size(); ++i) {
    
//     odbcHandler.CloseStmt();
//     odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, operations_query, vector<string> {}, COL1_NAME + "=" + inserted_pk[i]));
//     ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

//     rcode = SQLFetch(odbcHandler.GetStatementHandle());
//     ASSERT_EQ(rcode, SQL_SUCCESS);

//     for (int j = 0; j < operations_query.size(); j++) {

//       ASSERT_EQ(col_len[j], BYTES_EXPECTED);
//       ASSERT_EQ(col_results[j], expected_results[i][j]);
//     }
//   }

//   // Assert that there is no more data
//   rcode = SQLFetch(odbcHandler.GetStatementHandle());
//   ASSERT_EQ(rcode, SQL_NO_DATA);

//   odbcHandler.CloseStmt();
//   odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
// }

TEST_F(PSQL_DataTypes_Tinyint, Arithmetic_Operator) {

  const int BYTES_EXPECTED = 1;
  const int DOUBLE_BYTES_EXPECTED=8;
  int pk;
  unsigned char data;
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector <string> inserted_pk = {
    "4",
    "2"
  };

  vector <string> inserted_data = {
    "3",
    "5"
  };

  vector <string> operations_query = {
    COL1_NAME + "+" + COL2_NAME,
    COL1_NAME + "-" + COL2_NAME,
    COL1_NAME + "*" + COL2_NAME,
    COL1_NAME + "/" + COL2_NAME,
    "ABS(" + COL1_NAME + ")",
    "POWER(" + COL1_NAME+","+COL2_NAME + ")"
  };

  vector <string> double_operations_query= {
    "||/ "+COL1_NAME,
    "LOG(" + COL1_NAME + ")"
  };

  vector<vector<double>>double_expected_results = {{},{}};
  vector<vector<unsigned char>>expected_results = {{},{}};

  // initialization of expected_results
  for (int i = 0; i < inserted_pk.size(); i++) {
    expected_results[i].push_back(StringToTinyInt(inserted_pk[i]) + StringToTinyInt(inserted_data[i]));
    expected_results[i].push_back(StringToTinyInt(inserted_pk[i]) - StringToTinyInt(inserted_data[i]));
    expected_results[i].push_back(StringToTinyInt(inserted_pk[i]) * StringToTinyInt(inserted_data[i]));
    expected_results[i].push_back(StringToTinyInt(inserted_pk[i]) / StringToTinyInt(inserted_data[i]));
    expected_results[i].push_back(abs(StringToTinyInt(inserted_pk[i])));
    expected_results[i].push_back(pow(StringToTinyInt(inserted_pk[i]),StringToTinyInt(inserted_data[i])));
  }

  for (int i = 0; i < inserted_pk.size(); i++) {
    double_expected_results[i].push_back(cbrt(StringToTinyInt(inserted_pk[i])));
    double_expected_results[i].push_back(log10(StringToTinyInt(inserted_pk[i])));
    // std::cout <<double_expected_results[i][0]<<'\n';
  }

  double double_col_results[double_operations_query.size()];
  unsigned char col_results[operations_query.size()];
  SQLLEN col_len[operations_query.size()];
  SQLLEN double_col_len[double_operations_query.size()];
  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {};
  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> double_bind_columns = {};

  // initialization for bind_columns
  for (int i = 0; i < operations_query.size(); i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_STINYINT, (SQLPOINTER) &col_results[i], 0, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }
  for (int i = 0; i < double_operations_query.size(); i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_DOUBLE, (SQLPOINTER) &double_col_results[i], 0, &double_col_len[i]);
    double_bind_columns.push_back(tuple_to_insert);
  }

  string insert_string{}; 
  string comma{};
  
  // insert_string initialization
  for (int i = 0; i< inserted_pk.size() ; ++i) {
    insert_string += comma + "(" + inserted_pk[i] + "," + inserted_data[i] + ")";
    comma = ",";
  }

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_TINYINT));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));
 
  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_data.size());
  

  // Make sure inserted values are correct and operations
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_data.size(); ++i) {
    
    odbcHandler.CloseStmt();
    odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, operations_query, vector<string> {}, COL1_NAME + "=" + inserted_pk[i]));
    ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < operations_query.size(); j++) {

      ASSERT_EQ(col_len[j], BYTES_EXPECTED);
      ASSERT_EQ(col_results[j], expected_results[i][j]);
      // std::cout<<"Expected Result: "<< expected_results[i][j]<<'\n';
    }
  }
  for (int i = 0; i < inserted_data.size(); ++i) {
    
    odbcHandler.CloseStmt();
    odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, double_operations_query, vector<string> {}, COL1_NAME + "=" + inserted_pk[i]));
    ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(double_bind_columns));

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < double_operations_query.size(); j++) {

      ASSERT_EQ(double_col_len[j], DOUBLE_BYTES_EXPECTED);
      ASSERT_EQ(double_col_results[j], double_expected_results[i][j]);
      // std::cout<<"Expected Result: "<< double_expected_results[i][j]<<'\n';
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}


TEST_F(PSQL_DataTypes_Tinyint, View_Creation) {

  const string VIEW_QUERY = "SELECT * FROM " + TABLE_NAME;

  const int BYTES_EXPECTED = 1;

  unsigned char pk;
  unsigned char data;
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector <string> valid_inserted_values = {
    "0",
    "255",
    "3",
    "NULL"
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {
    {1, SQL_C_STINYINT, &pk, 0, &pk_len},
    {2, SQL_C_STINYINT, &data, 0,  &data_len}
  };

  string insert_string{}; 
  string comma{};
  
  for (int i = 0; i< valid_inserted_values.size(); ++i) {
    insert_string += comma + "(" + std::to_string(i) + "," + valid_inserted_values[i] + ")";
    comma = ",";
  }

  // Create Table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_TINYINT));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));
 
  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, valid_inserted_values.size());
  
  odbcHandler.CloseStmt();

  // Create view
  odbcHandler.ExecQuery(CreateViewStatement(VIEW_NAME, VIEW_QUERY));
  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(VIEW_NAME, {"*"}, vector<string> {COL1_NAME}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < valid_inserted_values.size(); ++i) {
    
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, BYTES_EXPECTED);
    ASSERT_EQ(pk, i);

    if (valid_inserted_values[i] != "NULL")
    {
      ASSERT_EQ(data_len, BYTES_EXPECTED);
      ASSERT_EQ(data, StringToTinyInt(valid_inserted_values[i]));
    }
    else 
      ASSERT_EQ(data_len, SQL_NULL_DATA);
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("VIEW", VIEW_NAME));

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Tinyint, Table_Unique_Constraints) {
  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL1_NAME, "INT PRIMARY KEY"},
    {COL2_NAME, DATATYPE_NAME + " UNIQUE"}
  };
  const string UNIQUE_COLUMN_NAME = COL2_NAME;

  const int PK_BYTES_EXPECTED = 4;
  const int DATA_BYTES_EXPECTED = 1;
  const int BUFFER_SIZE=256;

  int pk;
  unsigned char data;
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  const vector<string> VALID_INSERTED_VALUES = {
    "0",
    "1"
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_STINYINT, &data, 0, &data_len}
  };

  string insert_string{};
  string comma{};

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Check if unique constraint still matches after creation
  char column_name[BUFFER_SIZE];
  char type_name[BUFFER_SIZE];

  vector<tuple<int, int, SQLPOINTER, int>> table_BIND_COLUMNS = {
    {1, SQL_C_CHAR, column_name, BUFFER_SIZE},
  };
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(table_BIND_COLUMNS));

  const string PK_QUERY =
    "SELECT C.COLUMN_NAME FROM "
    "INFORMATION_SCHEMA.TABLE_CONSTRAINTS T "
    "JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C "
    "ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME "
    "WHERE "
    "C.TABLE_NAME='" + TABLE_NAME.substr(TABLE_NAME.find('.') + 1, TABLE_NAME.length()) + "' "
    "AND T.CONSTRAINT_TYPE='UNIQUE'";
  odbcHandler.ExecQuery(PK_QUERY);
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(string(column_name), UNIQUE_COLUMN_NAME);

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  EXPECT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));

  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, NUM_OF_INSERTS);

  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string>{COL1_NAME}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(BIND_COLUMNS));

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, PK_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);
    if (VALID_INSERTED_VALUES[i] != "NULL") {
      ASSERT_EQ(data_len, DATA_BYTES_EXPECTED);
      ASSERT_EQ(data, StringToTinyInt(VALID_INSERTED_VALUES[i]));
    }
    else {
      ASSERT_EQ(data_len, SQL_NULL_DATA);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();

  // Attempt to insert
  const vector<string> INVALID_INSERTED_VALUES = {
    "0",
    "1"
  };
  const int NUM_OF_INVALID = INVALID_INSERTED_VALUES.size();

  // Attempt to insert values that violates unique constraint and assert that they all fail
  for (int i = NUM_OF_INVALID; i < 2 * NUM_OF_INVALID; i++) {
    string insert_string = "(" + std::to_string(i) + "," + INVALID_INSERTED_VALUES[i - NUM_OF_INVALID] + ")";

    rcode = SQLExecDirect(odbcHandler.GetStatementHandle(), (SQLCHAR *)InsertStatement(TABLE_NAME, insert_string).c_str(), SQL_NTS);
    ASSERT_EQ(rcode, SQL_ERROR);
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Tinyint, Table_Composite_Keys) {
  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL1_NAME, DATATYPE_NAME},
    {COL2_NAME, DATATYPE_NAME}
  };
  const string PKTABLE_NAME = TABLE_NAME.substr(TABLE_NAME.find('.') + 1, TABLE_NAME.length());
  const string SCHEMA_NAME = TABLE_NAME.substr(0, TABLE_NAME.find('.'));

  const vector<string> PK_COLUMNS = {
    COL1_NAME,
    COL2_NAME
  };

  string table_constraints{"PRIMARY KEY ("};
  string comma{};
  for (int i = 0; i < PK_COLUMNS.size(); i++) {
    table_constraints += comma + PK_COLUMNS[i];
    comma = ",";
  }
  table_constraints += ")";

  const int PK_BYTES_EXPECTED = 4;
  const int DATA_BYTES_EXPECTED = 1;
  const int BUFFER_SIZE=256;

  int pk;
  unsigned char data;
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  const vector<string> VALID_INSERTED_VALUES = {
    "0",
    "1"
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  string insert_string{};
  comma = "";

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS, table_constraints));
  odbcHandler.CloseStmt();

  // Check if composite key still matches after creation
  char table_name[BUFFER_SIZE];
  char column_name[BUFFER_SIZE];
  int key_sq{};
  char pk_name[BUFFER_SIZE];

  const vector<tuple<int, int, SQLPOINTER, int>> CONSTRAINT_BIND_COLUMNS = {
    {3, SQL_C_CHAR, table_name, BUFFER_SIZE},
    {4, SQL_C_CHAR, column_name, BUFFER_SIZE},
    {5, SQL_C_STINYINT, &key_sq, BUFFER_SIZE},
    {6, SQL_C_CHAR, pk_name, BUFFER_SIZE}
  };
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(CONSTRAINT_BIND_COLUMNS));

  rcode = SQLPrimaryKeys(odbcHandler.GetStatementHandle(), NULL, 0, (SQLCHAR *)SCHEMA_NAME.c_str(), SQL_NTS, (SQLCHAR *)PKTABLE_NAME.c_str(), SQL_NTS);
  ASSERT_EQ(rcode, SQL_SUCCESS);

  int curr_sq{0};
  for (auto columnName : PK_COLUMNS) {
    ++curr_sq;
    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_SUCCESS);

    ASSERT_EQ(string(table_name), PKTABLE_NAME);
    ASSERT_EQ(string(column_name), columnName);
    ASSERT_EQ(key_sq, curr_sq);
  }
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));

  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, NUM_OF_INSERTS);

  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string>{COL1_NAME}));

  // Make sure inserted values are correct
  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_STINYINT, &data, 0, &data_len}
  };

  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(BIND_COLUMNS));

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, PK_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);
    if (VALID_INSERTED_VALUES[i] != "NULL") {
      ASSERT_EQ(data_len, DATA_BYTES_EXPECTED);
      ASSERT_EQ(data, StringToTinyInt(VALID_INSERTED_VALUES[i]));
    }
    else {
      ASSERT_EQ(data_len, SQL_NULL_DATA);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();

  // Attempt to insert values that violates composite constraint and assert that they all fail
  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

  rcode = SQLExecDirect(odbcHandler.GetStatementHandle(), (SQLCHAR *)InsertStatement(TABLE_NAME, insert_string).c_str(), SQL_NTS);
  ASSERT_EQ(rcode, SQL_ERROR);

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}