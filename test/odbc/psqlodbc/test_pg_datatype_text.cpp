#include <gtest/gtest.h>
#include <sqlext.h>
#include "../src/drivers.h"
#include "../src/odbc_handler.h"
#include "../src/query_generator.h"
#include <iostream>
#include <math.h>
using std::pair;

const string BBF_TABLE_NAME = "master.dbo.text_table_odbc_test";
const string PG_TABLE_NAME = "master_dbo.text_table_odbc_test";
const string DATATYPE_NAME = "text";
const string VIEW_NAME = "master_dbo.text_view_odbc_test";

const int NUM_COLS = 4;
const string COL_NAMES[NUM_COLS] = {"pk", "text_1", "text_4000", "text_20"};
const int BUFFER_LENGTH = 8192;
const int COL_LENGTH[NUM_COLS] = {8190, 8190, 8190, 8190};

const string STRING_4000 = "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";
  const string STRING_1 = "a";
  const string STRING_20 = "0123456789abcdefghij";

vector<pair<string, string>> TABLE_COLUMNS_TEXT = {
  {COL_NAMES[0], DATATYPE_NAME + " PRIMARY KEY"},
  {COL_NAMES[1], DATATYPE_NAME},
  {COL_NAMES[2], DATATYPE_NAME},
  {COL_NAMES[3], DATATYPE_NAME}
};

// BBF CONNECTION
class MSSQL_DataTypes_Text : public testing::Test{

  void SetUp() override {
    OdbcHandler test_setup(Drivers::GetDriver(ServerType::MSSQL));
    test_setup.ConnectAndExecQuery(DropObjectStatement("TABLE", BBF_TABLE_NAME));
  }

  void TearDown() override {

    OdbcHandler test_teardown(Drivers::GetDriver(ServerType::MSSQL));
    test_teardown.ConnectAndExecQuery(DropObjectStatement("VIEW", VIEW_NAME));
    test_teardown.CloseStmt();
    test_teardown.ExecQuery(DropObjectStatement("TABLE", BBF_TABLE_NAME));
  }
};

// PG CONNECTION
class PSQL_DataTypes_Text : public testing::Test{

  void SetUp() override {
    OdbcHandler test_setup(Drivers::GetDriver(ServerType::PSQL));
    test_setup.ConnectAndExecQuery(DropObjectStatement("TABLE", PG_TABLE_NAME));
  }

  void TearDown() override {

    OdbcHandler test_teardown(Drivers::GetDriver(ServerType::PSQL));
    test_teardown.ConnectAndExecQuery(DropObjectStatement("VIEW", VIEW_NAME));
    test_teardown.CloseStmt();
    test_teardown.ExecQuery(DropObjectStatement("TABLE", PG_TABLE_NAME));
  }
};

// helper function to initialize insert string (1, "", "", ""), etc.
string InitializeInsertString_Text(const vector<vector<string>> &inserted_values) {

  string insert_string{};
  string comma{};

  for (int i = 0; i< inserted_values.size(); ++i) {

    insert_string += comma + "(";
    string comma2{};

    for (int j = 0; j < NUM_COLS; j++) {
      if (inserted_values[i][j] != "NULL")
        insert_string += comma2 + "'" + inserted_values[i][j] + "'";
      else
        insert_string += comma2 + inserted_values[i][j];
      comma2 = ",";
    }

    insert_string += ")";
    comma = ",";
  }

  return insert_string;
}

string StringToLower(const string& s){
  string ret="";

  for(int i=0;i<s.size();i++){
    char cur=s[i]+32;
    ret= ret+cur;
  }
  return ret;
}

string operator_multiple_ntext(const string& s,unsigned int n){
  string ret;

  for(unsigned int i=0;i<n;i++){
    ret=ret+s;
  }
  return ret;
}

TEST_F(PSQL_DataTypes_Text, Table_Creation) {

  const int LENGTH_EXPECTED = 8190;
  const int PRECISION_EXPECTED = 0;
  const int SCALE_EXPECTED = 0;
  const string NAME_EXPECTED = "text";
  const int L_LENGTH_EXPECTED = 8190;

  const int BUFFER_SIZE = 256;
  char name[BUFFER_SIZE];
  SQLLEN length;
  SQLLEN precision;
  SQLLEN scale;

  RETCODE rcode;
  OdbcHandler BBF_OdbcHandler(Drivers::GetDriver(ServerType::MSSQL));
  OdbcHandler PG_OdbcHandler(Drivers::GetDriver(ServerType::PSQL));

  // Create a table with columns defined with the specific datatype being tested. 
  BBF_OdbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS_TEXT));
  BBF_OdbcHandler.CloseStmt();

  // Select * From Table to ensure that it exists
  PG_OdbcHandler.ConnectAndExecQuery(SelectStatement(PG_TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  // Make sure column attributes are correct
  rcode = SQLColAttribute(PG_OdbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_DISPLAY_SIZE, // Get the length of the column (size of char in columns)
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &length);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(length, LENGTH_EXPECTED);

  rcode = SQLColAttribute(PG_OdbcHandler.GetStatementHandle(),
                            2,
                            SQL_DESC_LENGTH, // Get the Bytes of the column 
                            NULL,
                            0,
                            NULL,
                            (SQLLEN*) &length);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(length, L_LENGTH_EXPECTED);

  rcode = SQLColAttribute(PG_OdbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_PRECISION, // Get the precision of the column
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &precision); 
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(precision, PRECISION_EXPECTED);

  rcode = SQLColAttribute(PG_OdbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_SCALE, // Get the scale of the column
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &scale); 
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(scale, SCALE_EXPECTED);

  rcode = SQLColAttribute(PG_OdbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_TYPE_NAME, // Get the type name of the column
                          name,
                          BUFFER_SIZE,
                          NULL,
                          NULL); 
  ASSERT_EQ(string(name), NAME_EXPECTED);

  rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  PG_OdbcHandler.CloseStmt();
  PG_OdbcHandler.ExecQuery(DropObjectStatement("TABLE", PG_TABLE_NAME));

}

TEST_F(MSSQL_DataTypes_Text, Table_Creation) {

  const int LENGTH_EXPECTED = 2147483647;
  const int PRECISION_EXPECTED = 2147483647;
  const int SCALE_EXPECTED = 0;
  const string NAME_EXPECTED = "text";
  const int L_LENGTH_EXPECTED = 2147483647;

  const int BUFFER_SIZE = 256;
  char name[BUFFER_SIZE];
  SQLLEN length;
  SQLLEN precision;
  SQLLEN scale;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::MSSQL));

  // Create a table with columns defined with the specific datatype being tested. 
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS_TEXT));
  odbcHandler.CloseStmt();

  // Select * From Table to ensure that it exists
  odbcHandler.ExecQuery(SelectStatement(BBF_TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  // Make sure column attributes are correct
  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_DISPLAY_SIZE, // Get the length of the column (size of char in columns)
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &length);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(length, LENGTH_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            2,
                            SQL_DESC_LENGTH, // Get the Bytes of the column 
                            NULL,
                            0,
                            NULL,
                            (SQLLEN*) &length);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(length, L_LENGTH_EXPECTED);

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
  ASSERT_EQ(string(name), NAME_EXPECTED);

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", BBF_TABLE_NAME));

}

TEST_F(MSSQL_DataTypes_Text, Table_Create_Fail) {

  vector<vector<pair<string, string>>> invalid_columns{
    {{"invalid1", DATATYPE_NAME + "(-1)"}},
    {{"invalid2", DATATYPE_NAME + "(0)"}},
    // {{"invalid3", DATATYPE + "(8001)"}}, -- This works on the postgres endpoint?
    {{"invalid4", DATATYPE_NAME + "(NULL)"}}
  };

  RETCODE rcode;
  OdbcHandler BBF_OdbcHandler(Drivers::GetDriver(ServerType::MSSQL));

  // Create a table with columns defined with the specific datatype being tested. 
  BBF_OdbcHandler.Connect();
  BBF_OdbcHandler.AllocateStmtHandle();

  // Assert that table creation will always fail with invalid column definitions
  for (int i = 0; i < invalid_columns.size(); i++) {
    rcode = SQLExecDirect(BBF_OdbcHandler.GetStatementHandle(),
                        (SQLCHAR*) CreateTableStatement(BBF_TABLE_NAME, invalid_columns[i]).c_str(),
                        SQL_NTS);

    ASSERT_EQ(rcode, SQL_ERROR);
  }

  BBF_OdbcHandler.CloseStmt();
  BBF_OdbcHandler.ExecQuery(DropObjectStatement("TABLE", BBF_TABLE_NAME));

}

TEST_F(PSQL_DataTypes_Text, Insertion_Success) {

  const int BUFFER_LENGTH = 8192;

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler PG_OdbcHandler(Drivers::GetDriver(ServerType::PSQL));
  OdbcHandler BBF_OdbcHandler(Drivers::GetDriver(ServerType::MSSQL));
  vector<vector<string>> inserted_values = {
    {"1", "", "", "" }, // empty strings
    {"2", STRING_1, STRING_4000, STRING_20}, // max values
    {"3", "a", "def", "ghi"}, // regular values
    {"4", "NULL", "NULL", "NULL"} // NULL values
  };
  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString_Text(inserted_values);

  // Create table
  BBF_OdbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS_TEXT));
  BBF_OdbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  PG_OdbcHandler.ConnectAndExecQuery(InsertStatement(PG_TABLE_NAME, insert_string));
 
  rcode = SQLRowCount(PG_OdbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_values.size());
  
  PG_OdbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  PG_OdbcHandler.ExecQuery(SelectStatement(PG_TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(PG_OdbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_values.size(); ++i) {
    
    rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < NUM_COLS; j++) {
      if (inserted_values[i][j] != "NULL") {
        ASSERT_EQ(string(col_results[j]), inserted_values[i][j]);
        ASSERT_EQ(col_len[j], inserted_values[i][j].size());
      } 
      else 
        ASSERT_EQ(col_len[j], SQL_NULL_DATA);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  PG_OdbcHandler.CloseStmt();
  PG_OdbcHandler.ExecQuery(DropObjectStatement("TABLE", PG_TABLE_NAME));

}
 
TEST_F(MSSQL_DataTypes_Text, Insertion_Success) {

  const int BUFFER_LENGTH = 8192;

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler BBF_OdbcHandler(Drivers::GetDriver(ServerType::MSSQL));
  vector<vector<string>> inserted_values = {
    {"1", "", "", "" }, // empty strings
    {"2", STRING_1, STRING_4000, STRING_20}, // max values
    {"3", "a", "def", "ghi"}, // regular values
    {"4", "NULL", "NULL", "NULL"} // NULL values
  };
  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString_Text(inserted_values);

  // Create table
  BBF_OdbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS_TEXT));
  BBF_OdbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  BBF_OdbcHandler.ExecQuery(InsertStatement(BBF_TABLE_NAME, insert_string));
 
  rcode = SQLRowCount(BBF_OdbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_values.size());
  
  BBF_OdbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  BBF_OdbcHandler.ExecQuery(SelectStatement(BBF_TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(BBF_OdbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_values.size(); ++i) {
    
    rcode = SQLFetch(BBF_OdbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < NUM_COLS; j++) {
      if (inserted_values[i][j] != "NULL") {
        ASSERT_EQ(string(col_results[j]), inserted_values[i][j]);
        ASSERT_EQ(col_len[j], inserted_values[i][j].size());
      } 
      else 
        ASSERT_EQ(col_len[j], SQL_NULL_DATA);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(BBF_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  BBF_OdbcHandler.CloseStmt();
  BBF_OdbcHandler.ExecQuery(DropObjectStatement("TABLE", BBF_TABLE_NAME));

}

TEST_F(PSQL_DataTypes_Text, Update_Success) {

  const int AFFECTED_ROWS_EXPECTED = 1;
  const string PK_VAL = "1";

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler BBF_OdbcHandler(Drivers::GetDriver(ServerType::MSSQL));
  OdbcHandler PG_OdbcHandler(Drivers::GetDriver(ServerType::PSQL));

  vector<vector<string>> inserted_values = {
    {PK_VAL, "1", "2", "3"} 
  };

  vector<vector<string>> updated_values = {
    {PK_VAL, "a", "b", "c"}, // standard values
    {PK_VAL, STRING_1, STRING_4000, STRING_20}, // max values
    {PK_VAL, "", "", ""} // min values
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i + 1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString_Text(inserted_values);

  // Create table
  BBF_OdbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS_TEXT));
  BBF_OdbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  PG_OdbcHandler.ConnectAndExecQuery(InsertStatement(PG_TABLE_NAME, insert_string));
 
  rcode = SQLRowCount(PG_OdbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_values.size());
  
  PG_OdbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  PG_OdbcHandler.ExecQuery(SelectStatement(PG_TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(PG_OdbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_values.size(); ++i) {
    rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < NUM_COLS; j++) {
      ASSERT_EQ(string(col_results[j]), inserted_values[i][j]);
      ASSERT_EQ(col_len[j], inserted_values[i][j].size());
    }
  }

  rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);
  PG_OdbcHandler.CloseStmt();

  for (int i = 0; i < updated_values.size(); i++) {

    vector<pair<string,string>> update_col;
    // setup update column
    for (int j = 0; j <NUM_COLS; j++) {
      string value = string("'") + updated_values[i][j] + string("'");
      update_col.push_back(pair<string,string>(COL_NAMES[j], value));
    }

    PG_OdbcHandler.ExecQuery(UpdateTableStatement(PG_TABLE_NAME, update_col, COL_NAMES[0] + "='" + PK_VAL + "'"));

    rcode = SQLRowCount(PG_OdbcHandler.GetStatementHandle(), &affected_rows);
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(affected_rows, AFFECTED_ROWS_EXPECTED);

    PG_OdbcHandler.CloseStmt();

    PG_OdbcHandler.ExecQuery(SelectStatement(PG_TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));
    rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());

    for (int j = 0; j < NUM_COLS; j++) {
      ASSERT_EQ(string(col_results[j]), updated_values[i][j]);
      ASSERT_EQ(col_len[j], updated_values[i][j].size());
    }

    rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_NO_DATA);
    PG_OdbcHandler.CloseStmt();
  }

  PG_OdbcHandler.ExecQuery(DropObjectStatement("TABLE", PG_TABLE_NAME));
}

TEST_F(MSSQL_DataTypes_Text, Update_Success) {

  const int AFFECTED_ROWS_EXPECTED = 1;
  const string PK_VAL = "1";

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::MSSQL));

  vector<vector<string>> inserted_values = {
    {PK_VAL, "1", "2", "3"} 
  };

  vector<vector<string>> updated_values = {
    {PK_VAL, "a", "b", "c"}, // standard values
    {PK_VAL, STRING_1, STRING_4000, STRING_20}, // max values
    {PK_VAL, "", "", ""} // min values
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i + 1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString_Text(inserted_values);

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS_TEXT));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(BBF_TABLE_NAME, insert_string));
 
  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_values.size());
  
  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(BBF_TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_values.size(); ++i) {
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < NUM_COLS; j++) {
      ASSERT_EQ(string(col_results[j]), inserted_values[i][j]);
      ASSERT_EQ(col_len[j], inserted_values[i][j].size());
    }
  }

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);
  odbcHandler.CloseStmt();

  for (int i = 0; i < updated_values.size(); i++) {

    vector<pair<string,string>> update_col;
    // setup update column
    for (int j = 0; j <NUM_COLS; j++) {
      string value = string("'") + updated_values[i][j] + string("'");
      update_col.push_back(pair<string,string>(COL_NAMES[j], value));
    }

    odbcHandler.ExecQuery(UpdateTableStatement(BBF_TABLE_NAME, update_col, COL_NAMES[0] + "='" + PK_VAL + "'"));

    rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(affected_rows, AFFECTED_ROWS_EXPECTED);

    odbcHandler.CloseStmt();

    odbcHandler.ExecQuery(SelectStatement(BBF_TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));
    rcode = SQLFetch(odbcHandler.GetStatementHandle());

    for (int j = 0; j < NUM_COLS; j++) {
      ASSERT_EQ(string(col_results[j]), updated_values[i][j]);
      ASSERT_EQ(col_len[j], updated_values[i][j].size());
    }

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_NO_DATA);
    odbcHandler.CloseStmt();
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", BBF_TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Text, String_Operators) {

  const int BYTES_EXPECTED = 4;
  const int DOUBLE_BYTES_EXPECTE = 8;
  int pk;
  char data[BUFFER_LENGTH];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler BBF_OdbcHandler(Drivers::GetDriver(ServerType::MSSQL));
  OdbcHandler PG_OdbcHandler(Drivers::GetDriver(ServerType::PSQL));

  const int NUM_COLS = 2;
  const string COL_NAMES[NUM_COLS] = {"pk", "data"};
  const int BUFFER_LENGTH = 8192;
  const int COL_LENGTH[NUM_COLS] = {8190, 8190};

  vector<pair<string, string>> TABLE_COLUMNS_TEXT = {
    {COL_NAMES[0], DATATYPE_NAME + " PRIMARY KEY"},
    {COL_NAMES[1], DATATYPE_NAME}
  };

  vector <string> inserted_pk = {
    "123",
    "456"
  };

  vector <string> inserted_data = {
    "One Two Three",
    "Four Five Six"
  };

  vector <string> operations_query = {
    // BBF does not support || for concat function
    "CONCAT("+COL_NAMES[0]+ "," + COL_NAMES[1]+')',
    "lower("+COL_NAMES[1]+")",
    COL_NAMES[0] + ">" + COL_NAMES[1],
    COL_NAMES[0] + ">=" + COL_NAMES[1],
    COL_NAMES[0] + "<=" + COL_NAMES[1],
    COL_NAMES[0] + "<" + COL_NAMES[1],
    COL_NAMES[0] + "<>" + COL_NAMES[1]
  };


  vector<vector<string>>expected_results = {{},{}};

  // initialization of expected_results
  for (int i = 0; i < inserted_pk.size(); i++) {
    expected_results[i].push_back(inserted_pk[i] + inserted_data[i]);
    string current=inserted_data[i];
    transform(current.begin(), current.end(), current.begin(), ::tolower);
    expected_results[i].push_back(current);
    // BBF does not support any comparison operators like <,>,=
    expected_results[i].push_back(std::to_string(inserted_pk[i] > inserted_data[i]));
    expected_results[i].push_back(std::to_string(inserted_pk[i] >= inserted_data[i]));
    expected_results[i].push_back(std::to_string(inserted_pk[i] <= inserted_data[i]));
    expected_results[i].push_back(std::to_string(inserted_pk[i] < inserted_data[i]));
    expected_results[i].push_back(std::to_string(inserted_pk[i] != inserted_data[i]));
  }

  char col_results[operations_query.size()][BUFFER_LENGTH];
  SQLLEN col_len[operations_query.size()];
  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {};

  // initialization for bind_columns
  for (int i = 0; i < operations_query.size(); i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i + 1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string{}; 
  string comma{};
  
  // insert_string initialization
  for (int i = 0; i< inserted_pk.size() ; ++i) {
    insert_string += comma + "(" +"'"+ inserted_pk[i] + "'"+","+"'" + inserted_data[i] + "'"+")";
    comma = ",";
  }

  // Create table
  BBF_OdbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS_TEXT));
  BBF_OdbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  PG_OdbcHandler.ConnectAndExecQuery(InsertStatement(PG_TABLE_NAME, insert_string));
  rcode = SQLRowCount(PG_OdbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_data.size());
  

  // Make sure inserted values are correct and operations
  ASSERT_NO_FATAL_FAILURE(PG_OdbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_data.size(); ++i) {
    
    PG_OdbcHandler.CloseStmt();
    PG_OdbcHandler.ExecQuery(SelectStatement(PG_TABLE_NAME, operations_query, vector<string> {}, COL_NAMES[0] + "=" + "'" + inserted_pk[i]+"'"));
    ASSERT_NO_FATAL_FAILURE(PG_OdbcHandler.BindColumns(bind_columns));

    rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < operations_query.size(); j++) {

      ASSERT_EQ(col_len[j], expected_results[i][j].size());
      ASSERT_EQ(col_results[j], expected_results[i][j]);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  PG_OdbcHandler.CloseStmt();
  PG_OdbcHandler.ExecQuery(DropObjectStatement("TABLE", PG_TABLE_NAME));
}

TEST_F(MSSQL_DataTypes_Text, String_Operators) {

  const int BYTES_EXPECTED = 4;
  const int DOUBLE_BYTES_EXPECTE = 8;
  int pk;
  char data[BUFFER_LENGTH];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::MSSQL));
  const int NUM_COLS = 2;
  const string COL_NAMES[NUM_COLS] = {"pk", "data"};
  const int BUFFER_LENGTH = 8192;
  const int COL_LENGTH[NUM_COLS] = {8190, 8190};

  vector<pair<string, string>> TABLE_COLUMNS_TEXT = {
    {COL_NAMES[0], DATATYPE_NAME + " PRIMARY KEY"},
    {COL_NAMES[1], DATATYPE_NAME}
  };

  vector <string> inserted_pk = {
    "123",
    "456"
  };

  vector <string> inserted_data = {
    "One Two Three",
    "Four Five Six"
  };

  vector <string> operations_query = {
    // BBF does not support || for concat function
    "CONCAT("+COL_NAMES[0]+ "," + COL_NAMES[1]+')',
    "lower("+COL_NAMES[1]+")",
    COL_NAMES[0]+ "+" + COL_NAMES[1],
    // COL_NAMES[0] + ">" + COL_NAMES[1],
    // COL_NAMES[0] + ">=" + COL_NAMES[1],
    // COL_NAMES[0] + "<=" + COL_NAMES[1],
    // COL_NAMES[0] + "<" + COL_NAMES[1],
    // COL_NAMES[0] + "<>" + COL_NAMES[1]
  };


  vector<vector<string>>expected_results = {{},{}};

  // initialization of expected_results
  for (int i = 0; i < inserted_pk.size(); i++) {
    expected_results[i].push_back(inserted_pk[i] + inserted_data[i]);
    string current=inserted_data[i];
    transform(current.begin(), current.end(), current.begin(), ::tolower);
    expected_results[i].push_back(current);
    expected_results[i].push_back(inserted_pk[i] + inserted_data[i]);
    // BBF does not support any comparison operators like <,>,=
    // expected_results[i].push_back(std::to_string(inserted_pk[i] > inserted_data[i]));
    // expected_results[i].push_back(std::to_string(inserted_pk[i] >= inserted_data[i]));
    // expected_results[i].push_back(std::to_string(inserted_pk[i] <= inserted_data[i]));
    // expected_results[i].push_back(std::to_string(inserted_pk[i] < inserted_data[i]));
    // expected_results[i].push_back(std::to_string(inserted_pk[i] != inserted_data[i]));
  }

  char col_results[operations_query.size()][BUFFER_LENGTH];
  SQLLEN col_len[operations_query.size()];
  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {};

  // initialization for bind_columns
  for (int i = 0; i < operations_query.size(); i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i + 1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string{}; 
  string comma{};
  
  // insert_string initialization
  for (int i = 0; i< inserted_pk.size() ; ++i) {
    insert_string += comma + "(" +"'"+ inserted_pk[i] + "'"+","+"'" + inserted_data[i] + "'"+")";
    comma = ",";
  }

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS_TEXT));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(BBF_TABLE_NAME, insert_string));
  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_data.size());
  

  // Make sure inserted values are correct and operations
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_data.size(); ++i) {
    
    odbcHandler.CloseStmt();
    odbcHandler.ExecQuery(SelectStatement(BBF_TABLE_NAME, operations_query, vector<string> {}, COL_NAMES[0] + "=" + "'" + inserted_pk[i]+"'"));
    ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < operations_query.size(); j++) {

      ASSERT_EQ(col_len[j], expected_results[i][j].size());
      ASSERT_EQ(col_results[j], expected_results[i][j]);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", BBF_TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Text, View_creation) {

  const string VIEW_QUERY = "SELECT * FROM " + PG_TABLE_NAME;
  const int BUFFER_LENGTH = 8192;

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler PG_OdbcHandler(Drivers::GetDriver(ServerType::PSQL));
  OdbcHandler BBF_OdbcHandler(Drivers::GetDriver(ServerType::MSSQL));

  vector<vector<string>> inserted_values = {
    {"1", "", "", "" }, // empty strings
    {"2", STRING_1, STRING_4000, STRING_20}, // max values
    {"3", "a", "def", "ghi"}, // regular values
    {"4", "NULL", "NULL", "NULL"} // NULL values
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i + 1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString_Text(inserted_values);

  // Create table
  BBF_OdbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS_TEXT));
  BBF_OdbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  PG_OdbcHandler.ConnectAndExecQuery(InsertStatement(PG_TABLE_NAME, insert_string));
 
  rcode = SQLRowCount(PG_OdbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_values.size());
  
  PG_OdbcHandler.CloseStmt();

  // Create view
  PG_OdbcHandler.ExecQuery(CreateViewStatement(VIEW_NAME, VIEW_QUERY));
  PG_OdbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  PG_OdbcHandler.ExecQuery(SelectStatement(VIEW_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(PG_OdbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_values.size(); ++i) {
    
    rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < NUM_COLS; j++) {
      
      if (inserted_values[i][j] != "NULL") {
        ASSERT_EQ(string(col_results[j]), inserted_values[i][j]);
        ASSERT_EQ(col_len[j], inserted_values[i][j].size());
      } 
      else {
        ASSERT_EQ(col_len[j], SQL_NULL_DATA);
      }
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  PG_OdbcHandler.CloseStmt();
  PG_OdbcHandler.ExecQuery(DropObjectStatement("VIEW", VIEW_NAME));
}

TEST_F(PSQL_DataTypes_Text, Table_Composite_Keys) {

  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL_NAMES[0], "int" },
    {COL_NAMES[1], DATATYPE_NAME}
  };
  const string PKTABLE_NAME = PG_TABLE_NAME.substr(PG_TABLE_NAME.find('.') + 1, PG_TABLE_NAME.length());
  const string SCHEMA_NAME = PG_TABLE_NAME.substr(0, PG_TABLE_NAME.find('.'));

  const vector<string> PK_COLUMNS = {
    COL_NAMES[0],
    COL_NAMES[1]
  };

  string table_constraints{"PRIMARY KEY ("};
  string comma{};
  for (int i = 0; i < PK_COLUMNS.size(); i++) {
    table_constraints += comma + PK_COLUMNS[i];
    comma = ",";
  };
  table_constraints += ")";

  const int PK_BYTES_EXPECTED = 4;

  int pk;
  char data[BUFFER_LENGTH];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler PG_OdbcHandler(Drivers::GetDriver(ServerType::PSQL));
  OdbcHandler BBF_OdbcHandler(Drivers::GetDriver(ServerType::MSSQL));

  const vector<string> VALID_INSERTED_VALUES = {
    STRING_1,
    STRING_1,
    STRING_20,
    STRING_20
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  string insert_string{};
  comma = "";

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + ", '" + VALID_INSERTED_VALUES[i] + "')";
    comma = ",";
  }

  BBF_OdbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS, table_constraints));
  BBF_OdbcHandler.CloseStmt();
  PG_OdbcHandler.Connect(true);

  // Check if composite key still matches after creation
  char table_name[BUFFER_LENGTH];
  char column_name[BUFFER_LENGTH];
  int key_sq{};
  char pk_name[BUFFER_LENGTH];

  const vector<tuple<int, int, SQLPOINTER, int>> CONSTRAINT_BIND_COLUMNS = {
    {3, SQL_C_CHAR, table_name, BUFFER_LENGTH},
    {4, SQL_C_CHAR, column_name, BUFFER_LENGTH},
    {5, SQL_C_ULONG, &key_sq, BUFFER_LENGTH},
    {6, SQL_C_CHAR, pk_name, BUFFER_LENGTH}
  };
  ASSERT_NO_FATAL_FAILURE(PG_OdbcHandler.BindColumns(CONSTRAINT_BIND_COLUMNS));

  rcode = SQLPrimaryKeys(PG_OdbcHandler.GetStatementHandle(), NULL, 0, (SQLCHAR *)SCHEMA_NAME.c_str(), SQL_NTS, (SQLCHAR *)PKTABLE_NAME.c_str(), SQL_NTS);
  ASSERT_EQ(rcode, SQL_SUCCESS);

  int curr_sq{0};
  for (auto columnName : PK_COLUMNS) {
    ++curr_sq;
    rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_SUCCESS);

    ASSERT_EQ(string(table_name), PKTABLE_NAME);
    ASSERT_EQ(string(column_name), columnName);
    ASSERT_EQ(key_sq, curr_sq);
  }
  rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);
  PG_OdbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  PG_OdbcHandler.ExecQuery(InsertStatement(PG_TABLE_NAME, insert_string));

  rcode = SQLRowCount(PG_OdbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, NUM_OF_INSERTS);

  PG_OdbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  PG_OdbcHandler.ExecQuery(SelectStatement(PG_TABLE_NAME, {"*"}, vector<string>{COL_NAMES[0]}));

  // Make sure inserted values are correct
  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_LENGTH, &data_len}
  };

  ASSERT_NO_FATAL_FAILURE(PG_OdbcHandler.BindColumns(BIND_COLUMNS));

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, PK_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);
    if (VALID_INSERTED_VALUES[i] != "NULL") {
      ASSERT_EQ(data_len, VALID_INSERTED_VALUES[i].size());
      ASSERT_EQ(data, VALID_INSERTED_VALUES[i]);
    }
    else {
      ASSERT_EQ(data_len, SQL_NULL_DATA);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  PG_OdbcHandler.CloseStmt();

  // Attempt to insert values that violates composite constraint and assert that they all fail
  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

  rcode = SQLExecDirect(PG_OdbcHandler.GetStatementHandle(), (SQLCHAR *)InsertStatement(PG_TABLE_NAME, insert_string).c_str(), SQL_NTS);
  ASSERT_EQ(rcode, SQL_ERROR);

  PG_OdbcHandler.ExecQuery(DropObjectStatement("TABLE", PG_TABLE_NAME));

}

TEST_F(MSSQL_DataTypes_Text, Table_Composite_Keys) {

  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL_NAMES[0], "int" },
    {COL_NAMES[1], DATATYPE_NAME}
  };
  size_t first_period = BBF_TABLE_NAME.find('.') + 1;
  size_t second_period = BBF_TABLE_NAME.find('.', first_period);
  const string SCHEMA_NAME = BBF_TABLE_NAME.substr(first_period, second_period - first_period);
  const string PKBBF_TABLE_NAME = BBF_TABLE_NAME.substr(second_period + 1, BBF_TABLE_NAME.length() - second_period);

  const vector<string> PK_COLUMNS = {
    COL_NAMES[0],
    COL_NAMES[1]
  };

  string table_constraints{"PRIMARY KEY ("};
  string comma{};
  for (int i = 0; i < PK_COLUMNS.size(); i++) {
    table_constraints += comma + PK_COLUMNS[i];
    comma = ",";
  };
  table_constraints += ")";

  const int PK_BYTES_EXPECTED = 4;

  int pk;
  char data[BUFFER_LENGTH];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler BBF_OdbcHandler(Drivers::GetDriver(ServerType::MSSQL));

  const vector<string> VALID_INSERTED_VALUES = {
    STRING_1,
    STRING_1,
    STRING_20,
    STRING_20
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  string insert_string{};
  comma = "";

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + ", '" + VALID_INSERTED_VALUES[i] + "')";
    comma = ",";
  }

  BBF_OdbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS, table_constraints));
  BBF_OdbcHandler.CloseStmt();

  // Check if composite key still matches after creation
  char table_name[BUFFER_LENGTH];
  char column_name[BUFFER_LENGTH];
  int key_sq{};
  char pk_name[BUFFER_LENGTH];

  const vector<tuple<int, int, SQLPOINTER, int>> CONSTRAINT_BIND_COLUMNS = {
    {3, SQL_C_CHAR, table_name, BUFFER_LENGTH},
    {4, SQL_C_CHAR, column_name, BUFFER_LENGTH},
    {5, SQL_C_ULONG, &key_sq, BUFFER_LENGTH},
    {6, SQL_C_CHAR, pk_name, BUFFER_LENGTH}
  };
  ASSERT_NO_FATAL_FAILURE(BBF_OdbcHandler.BindColumns(CONSTRAINT_BIND_COLUMNS));

  rcode = SQLPrimaryKeys(BBF_OdbcHandler.GetStatementHandle(), NULL, 0, (SQLCHAR *)SCHEMA_NAME.c_str(), SQL_NTS, (SQLCHAR *)PKBBF_TABLE_NAME.c_str(), SQL_NTS);
  ASSERT_EQ(rcode, SQL_SUCCESS);

  int curr_sq{0};
  for (auto columnName : PK_COLUMNS) {
    ++curr_sq;
    rcode = SQLFetch(BBF_OdbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_SUCCESS);

    ASSERT_EQ(string(table_name), PKBBF_TABLE_NAME);
    ASSERT_EQ(string(column_name), columnName);
    ASSERT_EQ(key_sq, curr_sq);
  }
  rcode = SQLFetch(BBF_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);
  BBF_OdbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  BBF_OdbcHandler.ExecQuery(InsertStatement(BBF_TABLE_NAME, insert_string));

  rcode = SQLRowCount(BBF_OdbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, NUM_OF_INSERTS);

  BBF_OdbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  BBF_OdbcHandler.ExecQuery(SelectStatement(BBF_TABLE_NAME, {"*"}, vector<string>{COL_NAMES[0]}));

  // Make sure inserted values are correct
  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_LENGTH, &data_len}
  };

  ASSERT_NO_FATAL_FAILURE(BBF_OdbcHandler.BindColumns(BIND_COLUMNS));

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    rcode = SQLFetch(BBF_OdbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, PK_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);
    if (VALID_INSERTED_VALUES[i] != "NULL") {
      ASSERT_EQ(data_len, VALID_INSERTED_VALUES[i].size());
      ASSERT_EQ(data, VALID_INSERTED_VALUES[i]);
    }
    else {
      ASSERT_EQ(data_len, SQL_NULL_DATA);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(BBF_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  BBF_OdbcHandler.CloseStmt();

  // Attempt to insert values that violates composite constraint and assert that they all fail
  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

  rcode = SQLExecDirect(BBF_OdbcHandler.GetStatementHandle(), (SQLCHAR *)InsertStatement(BBF_TABLE_NAME, insert_string).c_str(), SQL_NTS);
  ASSERT_EQ(rcode, SQL_ERROR);

  BBF_OdbcHandler.ExecQuery(DropObjectStatement("TABLE", BBF_TABLE_NAME));

}

TEST_F(PSQL_DataTypes_Text, Table_Unique_Constraints) {

  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL_NAMES[0], "INT PRIMARY KEY" },
    {COL_NAMES[1], DATATYPE_NAME+ " UNIQUE NOT NULL"}
  };
 
  const string UNIQUE_COLUMN_NAME = COL_NAMES[1];

  const int PK_BYTES_EXPECTED = 4;

  int pk;
  char data[BUFFER_LENGTH];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler BBF_OdbcHandler(Drivers::GetDriver(ServerType::MSSQL));
  OdbcHandler PG_OdbcHandler(Drivers::GetDriver(ServerType::PSQL));

  const vector<string> VALID_INSERTED_VALUES = {
    "0",
    "1"
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_LENGTH, &data_len}
  };

  string insert_string{};
  string comma{};

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

  BBF_OdbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS));
  BBF_OdbcHandler.CloseStmt();
  PG_OdbcHandler.Connect(true);

  // Check if unique constraint still matches after creation
  char column_name[BUFFER_LENGTH];
  char type_name[BUFFER_LENGTH];

  vector<tuple<int, int, SQLPOINTER, int>> table_BIND_COLUMNS = {
    {1, SQL_C_CHAR, column_name, BUFFER_LENGTH},
  };
  ASSERT_NO_FATAL_FAILURE(PG_OdbcHandler.BindColumns(table_BIND_COLUMNS));

  const string PG_QUERY =
    "SELECT C.COLUMN_NAME FROM "
    "INFORMATION_SCHEMA.TABLE_CONSTRAINTS T "
    "JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C "
    "ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME "
    "WHERE "
    "C.TABLE_NAME='" + PG_TABLE_NAME.substr(PG_TABLE_NAME.find('.') + 1, PG_TABLE_NAME.length()) + "' "
    "AND T.CONSTRAINT_TYPE='UNIQUE'";
  PG_OdbcHandler.ExecQuery(PG_QUERY);
  rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(string(column_name), UNIQUE_COLUMN_NAME);

  rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
  EXPECT_EQ(rcode, SQL_NO_DATA);

  PG_OdbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  PG_OdbcHandler.ExecQuery(InsertStatement(PG_TABLE_NAME, insert_string));

  rcode = SQLRowCount(PG_OdbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, NUM_OF_INSERTS);

  PG_OdbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  PG_OdbcHandler.ExecQuery(SelectStatement(PG_TABLE_NAME, {"*"}, vector<string>{COL_NAMES[1]}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(PG_OdbcHandler.BindColumns(BIND_COLUMNS));

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, PK_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);

    if (VALID_INSERTED_VALUES[i] != "NULL") {
      ASSERT_EQ(data_len, VALID_INSERTED_VALUES[i].size());
      ASSERT_EQ(data, VALID_INSERTED_VALUES[i]);
    }
    else {
      ASSERT_EQ(data_len, SQL_NULL_DATA);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(PG_OdbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  PG_OdbcHandler.CloseStmt();

  // Attempt to insert
  const vector<string> INVALID_INSERTED_VALUES = {
    "0",
    "1"
  };
  const int NUM_OF_INVALID = INVALID_INSERTED_VALUES.size();

  // Attempt to insert values that violates unique constraint and assert that they all fail
  for (int i = NUM_OF_INVALID; i < 2 * NUM_OF_INVALID; i++) {
    string insert_string = "(" + std::to_string(i) + "," + INVALID_INSERTED_VALUES[i - NUM_OF_INVALID] + ")";

    rcode = SQLExecDirect(PG_OdbcHandler.GetStatementHandle(), (SQLCHAR *)InsertStatement(PG_TABLE_NAME, insert_string).c_str(), SQL_NTS);
    ASSERT_EQ(rcode, SQL_ERROR);
  }

  PG_OdbcHandler.ExecQuery(DropObjectStatement("TABLE", PG_TABLE_NAME));

}

TEST_F(MSSQL_DataTypes_Text, Table_Unique_Constraints) {

  size_t first_period = BBF_TABLE_NAME.find('.') + 1;
  size_t second_period = BBF_TABLE_NAME.find('.', first_period);  

  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL_NAMES[0], "INT PRIMARY KEY" },
    {COL_NAMES[1], DATATYPE_NAME+ " UNIQUE NOT NULL"}
  };
 
  const string UNIQUE_COLUMN_NAME = COL_NAMES[1];

  const int PK_BYTES_EXPECTED = 4;

  int pk;
  char data[BUFFER_LENGTH];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::MSSQL));

  const vector<string> VALID_INSERTED_VALUES = {
    "0",
    "1"
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_LENGTH, &data_len}
  };

  string insert_string{};
  string comma{};

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(BBF_TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Check if unique constraint still matches after creation
  char column_name[BUFFER_LENGTH];
  char type_name[BUFFER_LENGTH];

  vector<tuple<int, int, SQLPOINTER, int>> table_BIND_COLUMNS = {
    {1, SQL_C_CHAR, column_name, BUFFER_LENGTH},
  };
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(table_BIND_COLUMNS));

  const string BBF_QUERY =
    "SELECT C.COLUMN_NAME FROM "
    "INFORMATION_SCHEMA.TABLE_CONSTRAINTS T "
    "JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C "
    "ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME "
    "WHERE "
    "C.TABLE_NAME='" + BBF_TABLE_NAME.substr(second_period + 1, BBF_TABLE_NAME.length() - second_period) + "' "
    "AND T.CONSTRAINT_TYPE='UNIQUE'";
  odbcHandler.ExecQuery(BBF_QUERY);
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(string(column_name), UNIQUE_COLUMN_NAME);

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  EXPECT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(BBF_TABLE_NAME, insert_string));

  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, NUM_OF_INSERTS);

  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(BBF_TABLE_NAME, {"*"}, vector<string>{COL_NAMES[1]}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(BIND_COLUMNS));

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, PK_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);

    if (VALID_INSERTED_VALUES[i] != "NULL") {
      ASSERT_EQ(data_len, VALID_INSERTED_VALUES[i].size());
      ASSERT_EQ(data, VALID_INSERTED_VALUES[i]);
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

    rcode = SQLExecDirect(odbcHandler.GetStatementHandle(), (SQLCHAR *)InsertStatement(BBF_TABLE_NAME, insert_string).c_str(), SQL_NTS);
    ASSERT_EQ(rcode, SQL_ERROR);
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", BBF_TABLE_NAME));

}