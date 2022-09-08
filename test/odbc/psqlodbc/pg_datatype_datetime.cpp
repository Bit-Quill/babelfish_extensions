#include "../odbc_handler.h"
#include "../database_objects.h"
#include "../query_generator.h"

#include <gtest/gtest.h>
#include <sqlext.h>


using std::pair;

const string TABLE_NAME = "master_dbo.datetime_table_odbc_test";
const string VIEW_NAME = "master_dbo.datetime_view_odbc_test";
const string DATATYPE = "sys.datetime";
const int NUM_COLS = 2;
const string COL_NAMES[NUM_COLS] = {"pk", "datetime_1"};


vector<pair<string, string>> TABLE_COLUMNS = {
    {COL_NAMES[0], " int PRIMARY KEY"},
    {COL_NAMES[1], DATATYPE}
};

class PSQL_Datatypes_Datetime: public testing::Test {
   void SetUp() override {
    OdbcHandler test_setup(ServerType::PSQL);
    test_setup.ConnectAndExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
   }

   void TearDown() override {
    OdbcHandler test_cleanup(ServerType::PSQL);
    test_cleanup.ConnectAndExecQuery(DropObjectStatement("VIEW", VIEW_NAME));
    test_cleanup.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
   }

};

string InitializeInsertString(const vector<vector<string>> &inserted_values) {

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

TEST_F(PSQL_Datatypes_Datetime, Table_Creation) {

  const int LENGTH_EXPECTED = 255;
  const int PRECISION_EXPECTED = 0;
  const int SCALE_EXPECTED = 0;
  const string NAME_EXPECTED = "unknown";

  const int BUFFER_SIZE = 256;
  char name[BUFFER_SIZE];
  SQLLEN length;
  SQLLEN precision;
  SQLLEN scale;

  RETCODE rcode;
  OdbcHandler odbcHandler(ServerType::PSQL);

  // Create a table with columns defined with the specific datatype being tested. 

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Select * From Table to ensure that it exists
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  // Make sure column attributes are correct
  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_LENGTH, // Get the length of the column (size of char in columns)
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &length);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(length, LENGTH_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          2,
                          SQL_DESC_PRECISION, // Get the precision of the column
                          NULL,
                          2,
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
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

// Doesn't work in SQL Server, but does in BBF & BBF PG connection?
TEST_F(PSQL_Datatypes_Datetime, DISABLED_Table_Create_Fail) {

  vector<vector<pair<string, string>>> invalid_columns {
    {{"invalid1", DATATYPE + "(4)"}} // Cannot specify a column width on data type datetime.
  };

  RETCODE rcode;
  OdbcHandler odbcHandler(ServerType::PSQL);

  // Create a table with columns defined with the specific datatype being tested. 
  odbcHandler.Connect();
  odbcHandler.AllocateStmtHandle();

  // Assert that table creation will always fail with invalid column definitions
  for (int i = 0; i < invalid_columns.size(); i++) {
    rcode = SQLExecDirect(odbcHandler.GetStatementHandle(),
                        (SQLCHAR*) CreateTableStatement(TABLE_NAME, invalid_columns[i]).c_str(),
                        SQL_NTS);

    ASSERT_EQ(rcode, SQL_ERROR);
  }

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

// inserted values differ that of expected?
TEST_F(PSQL_Datatypes_Datetime, Insertion_Success) {

  const int BUFFER_LENGTH = 8192;

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(ServerType::PSQL);

  vector<vector<string>> inserted_values = {
    {"1", "NULL"}, // NULL values
    {"2", "1753-01-01 00:00:000" }, // smallest value
    {"3", "2011-04-15 16:44:09.000"}, // random regular values
    {"4", "9999-12-31 23:59:59.997"}, // max value
    {"5", ""} // blank value
  };

  vector<vector<string>> expected = {
    {"1", "NULL"}, // NULL values
    {"2", "1753-01-01 00:00:00" }, // smallest value
    {"3", "2011-04-15 16:44:09"}, // random regular value
    {"4", "9999-12-31 23:59:59.997"}, // max value
    {"5", "1900-01-01 00:00:00"} // blank value
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString(inserted_values);

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));

  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_values.size());

  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_values.size(); ++i) {

    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 1; j < NUM_COLS; j++) {
      if (inserted_values[i][j] != "NULL") {
        ASSERT_EQ(string(col_results[j]), expected[i][j]);
        ASSERT_EQ(col_len[j], expected[i][j].size());
      } 
      else 
        ASSERT_EQ(col_len[j], SQL_NULL_DATA);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_Datatypes_Datetime, Insertion_Failure) {

  const int BUFFER_LENGTH = 8192;

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(ServerType::PSQL);

  vector<vector<string>> inserted_values = {
    {"1", "1752-01-01 00:00:000" }, // past lowest boundary
    {"2", "9999-12-31 23:59:59.999"} // past highest boundary
  };


  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Insert invalid values in table and assert error
   for (int i = 0; i < inserted_values.size(); i++) {

    string insert_string = "(";
    string comma{};

    // create insert_string (1, ..., ..., ...)
    for (int j = 0; j < NUM_COLS; j++) {
      insert_string += comma + "'" + inserted_values[i][j] + "'";
      comma = ",";
    }
    insert_string += ")";

    rcode = SQLExecDirect(odbcHandler.GetStatementHandle(), (SQLCHAR*) InsertStatement(TABLE_NAME, insert_string).c_str(), SQL_NTS);
    ASSERT_EQ(rcode, SQL_ERROR);
    odbcHandler.CloseStmt();
  }

  // Select all from the table to make sure nothing was inserted
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_Datatypes_Datetime, Update_Success) {

  const int BUFFER_LENGTH = 8192;
  const int AFFECTED_ROWS_EXPECTED = 1;
  const string PK_VAL = "1";

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(ServerType::PSQL);

  vector<vector<string>> inserted_values = {
    {PK_VAL, "2011-04-15 16:44:09"} 
  };

  vector<vector<string>> updated_values = {
    {PK_VAL, "1900-01-31 12:59:59.999"}, // standard value
    {PK_VAL, "9999-12-31 23:59:59.997"}, // max value
    {PK_VAL, "1753-01-01 00:00:00"}, // min value
    {PK_VAL, ""} // blank value
  };

  vector<vector<string>> expected_updated_values = {
  {PK_VAL, "1900-01-31 12:59:59.999"}, // standard value
  {PK_VAL, "9999-12-31 23:59:59.997"}, // max value
  {PK_VAL, "1753-01-01 00:00:00"}, // min value
  {PK_VAL, "1900-01-01 00:00:00"} // blank value
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString(inserted_values);

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));

  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_values.size());

  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

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

    odbcHandler.ExecQuery(UpdateTableStatement(TABLE_NAME, update_col, COL_NAMES[0] + "='" + PK_VAL + "'"));

    rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(affected_rows, AFFECTED_ROWS_EXPECTED);

    odbcHandler.CloseStmt();

    odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));
    rcode = SQLFetch(odbcHandler.GetStatementHandle());

    for (int j = 0; j < NUM_COLS; j++) {
      ASSERT_EQ(string(col_results[j]), expected_updated_values[i][j]);
      ASSERT_EQ(col_len[j], expected_updated_values[i][j].size());
    }

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_NO_DATA);
    odbcHandler.CloseStmt();
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_Datatypes_Datetime, Update_Fail) {

  const int BUFFER_LENGTH = 8192;
  const int AFFECTED_ROWS_EXPECTED = 1;
  const string PK_VAL = "1";

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(ServerType::PSQL);

  vector<vector<string>> inserted_values = {
    {PK_VAL, "2011-04-15 16:44:09"} 
  };

  vector<vector<string>> updated_values = {
    {PK_VAL, "1752-01-01 00:00:000"} // past lowest boundary
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString(inserted_values);

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));

  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_values.size());

  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

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

  for (int i = 0; i < updated_values.size(); i++)
  {
    odbcHandler.CloseStmt();
    vector<pair<string,string>> update_col;

    // setup update column
    for (int j = 0; j <NUM_COLS; j++) {
      string value = string("'") + updated_values[i][j] + string("'");
      update_col.push_back(pair<string,string>(COL_NAMES[j], value));
    }

    // Update value and assert an error is present
    rcode = SQLExecDirect(odbcHandler.GetStatementHandle(),
                          (SQLCHAR*) UpdateTableStatement(TABLE_NAME, update_col, COL_NAMES[0] + "='" + PK_VAL + "'").c_str(), 
                          SQL_NTS);

    ASSERT_EQ(rcode, SQL_ERROR);

    odbcHandler.CloseStmt();

    odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));
    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_SUCCESS);

    // Assert that the results did not change
    for (int j = 0; j < NUM_COLS; j++) {
      ASSERT_EQ(string(col_results[j]), inserted_values[i][j]);
      ASSERT_EQ(col_len[j], inserted_values[i][j].size());
    }
  }
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_Datatypes_Datetime, View_creation) {

  const string VIEW_QUERY = "SELECT * FROM " + TABLE_NAME;
  const int BUFFER_LENGTH = 8192;

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(ServerType::PSQL);

  vector<vector<string>> inserted_values = {
    {"1", "NULL"}, // NULL values
    {"2", "1753-01-01 00:00:000" }, // smallest value
    {"3", "2011-04-15 16:44:09.000"}, // random regular values
    {"4", "9999-12-31 23:59:59.997"}, // max value
    {"5", ""} // blank value
  };

  vector<vector<string>> expected = {
    {"1", "NULL"}, // NULL values
    {"2", "1753-01-01 00:00:00" }, // smallest value
    {"3", "2011-04-15 16:44:09"}, // random regular value
    {"4", "9999-12-31 23:59:59.997"}, // max value
    {"5", "1900-01-01 00:00:00"} // blank value
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString(inserted_values);

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));

  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_values.size());

  odbcHandler.CloseStmt();

  // Create view
  odbcHandler.ExecQuery(CreateViewStatement(VIEW_NAME, VIEW_QUERY));
  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(VIEW_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_values.size(); ++i) {

    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < NUM_COLS; j++) {

      if (inserted_values[i][j] != "NULL") {

        ASSERT_EQ(string(col_results[j]), expected[i][j]);
        ASSERT_EQ(col_len[j], expected[i][j].size());
      } 
      else {
        ASSERT_EQ(col_len[j], SQL_NULL_DATA);
      }
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("VIEW", VIEW_NAME));
}