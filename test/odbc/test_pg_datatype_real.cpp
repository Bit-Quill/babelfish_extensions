#include <gtest/gtest.h>
#include <sqlext.h>
#include "odbc_handler.h"
#include "query_generator.h"
#include <cmath>
#include <iostream>
using std::pair;

const string TABLE_NAME = "master_dbo.real_table_odbc_test";
const string COL1_NAME = "pk";
const string COL2_NAME = "data";
const string DATATYPE_NAME = "sys.real";
const string VIEW_NAME = "master_dbo.real_view_odbc_test";
vector<pair<string, string>> TABLE_COLUMNS = {
  {COL1_NAME, DATATYPE_NAME + " PRIMARY KEY"},
  {COL2_NAME, DATATYPE_NAME}
};
const int DATA_COLUMN = 2;

class PSQL_DataTypes_Real : public testing::Test{
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

// Helper to convert string to float
float StringToFloat(const string &value) {
  return stof(value);
}

TEST_F(PSQL_DataTypes_Real, Table_Creation) {
  // TODO - Expected needs to be fixed.
  const int LENGTH_EXPECTED = 4;
  const int PRECISION_EXPECTED = 0;      // Double check, Expect 6?
  const int SCALE_EXPECTED = 0;
  const string NAME_EXPECTED = "float4"; // Double check
  
  const int BUFFER_SIZE = 256;
  char name[BUFFER_SIZE];
  SQLLEN length;
  SQLLEN precision;
  SQLLEN scale;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  // Create a table with columns defined with the specific datatype being tested. 
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Select * From Table to ensure that it exists
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL1_NAME}));

  // Make sure column attributes are correct
  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
                          SQL_DESC_LENGTH, // Get the length of the column (size of char in columns)
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &length);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(length, LENGTH_EXPECTED);
  
  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
                          SQL_DESC_PRECISION, // Get the precision of the column
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &precision); 
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(precision, PRECISION_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
                          SQL_DESC_SCALE, // Get the scale of the column
                          NULL,
                          0,
                          NULL,
                          (SQLLEN*) &scale); 
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(scale, SCALE_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
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

TEST_F(PSQL_DataTypes_Real, Insertion_Success) {  
  const int BYTES_EXPECTED = 4;

  float pk;
  float data;
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  // Range of around 1E-37 to 1E+37 with a precision of at least 6 decimal digits
  vector <string> valid_inserted_values = {
    "0",
    "0.123456",
    "-0.123456",
    "123456.123456",
    "0.123456789",
    "1E+37",
    "1E-37",
    "1e+10",
    "1e-20",
    "0000000000000000001",
    "-0123456789.12345",
    "NULL"
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN*>> bind_columns = {
    {1, SQL_C_FLOAT, &pk, 0, &pk_len},
    {2, SQL_C_FLOAT, &data, 0, &data_len}
  };

  string insert_string{}; 
  string comma{};

  for (int i = 0; i < valid_inserted_values.size(); i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + valid_inserted_values[i] + ")";
    comma = ",";
  }

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
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

  for (int i = 0; i < valid_inserted_values.size(); i++) {
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, BYTES_EXPECTED);
    ASSERT_EQ(pk, i);
    if (valid_inserted_values[i] != "NULL") {
      ASSERT_EQ(data_len, BYTES_EXPECTED);
      ASSERT_EQ(data, StringToFloat(valid_inserted_values[i]));
    }
    else {
      ASSERT_EQ(data_len, SQL_NULL_DATA);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Real, Insertion_Fail) {
  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector <string> invalid_inserted_values = {
    "1E+39", // 1E+38 is valid..?
    "1E-46", // 1E-46 is valid..?
    "999999999999999999999999999999999999999"
  };

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
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

TEST_F(PSQL_DataTypes_Real, Update_Success) {
  const string PK_INSERTED = "0";
  const string DATA_INSERTED = "0";
  const int NUM_UPDATES = 3;

  const string DATA_UPDATED_VALUES[NUM_UPDATES] = {
    "-0.123456",
    "0.123456789",
    "1E+37"
  };

  const string INSERT_STRING = "(" + PK_INSERTED + "," + DATA_INSERTED + ")";
  const string UPDATE_WHERE_CLAUSE = COL1_NAME + " = " + PK_INSERTED;

  const int BYTES_EXPECTED = 4;
  const int AFFECTED_ROWS_EXPECTED = 1;

  float pk;
  float data;
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {
    {1, SQL_C_FLOAT, &pk, 0, &pk_len},
    {2, SQL_C_FLOAT, &data, 0, &data_len}
  };

  vector<pair<string, string>> update_col{};

  for (int i = 0; i < NUM_UPDATES; i++) {
    update_col.push_back(pair<string, string>(COL2_NAME, DATA_UPDATED_VALUES[i]));
  }

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
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
  ASSERT_EQ(pk, StringToFloat(PK_INSERTED));
  ASSERT_EQ(data_len, BYTES_EXPECTED);
  ASSERT_EQ(data, StringToFloat(DATA_INSERTED));

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
    ASSERT_EQ(pk, StringToFloat(PK_INSERTED));
    ASSERT_EQ(data_len, BYTES_EXPECTED);
    ASSERT_EQ(data, StringToFloat(DATA_UPDATED_VALUES[i]));

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_NO_DATA);
    odbcHandler.CloseStmt();
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Real, Update_Fail) {
  const string PK_INSERTED = "0";
  const string DATA_INSERTED = "0";
  const string DATA_UPDATED_VALUE = "1E+10000";

  const string INSERT_STRING = "(" + PK_INSERTED + "," + DATA_INSERTED + ")";
  const string UPDATE_WHERE_CLAUSE = COL1_NAME + " = " + PK_INSERTED;

  const int BYTES_EXPECTED = 4;

  float pk;
  float data;
  SQLLEN pk_len;
  SQLLEN data_len;

  RETCODE rcode;
  OdbcHandler odbcHandler;  

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {
    {1, SQL_C_FLOAT, &pk, 0, &pk_len},
    {2, SQL_C_FLOAT, &data, 0, &data_len}
  };
  
  vector<pair<string, string>> update_col = {
    {COL2_NAME, DATA_UPDATED_VALUE}
  };

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
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
  ASSERT_EQ(pk, StringToFloat(PK_INSERTED));
  ASSERT_EQ(data_len, BYTES_EXPECTED);
  ASSERT_EQ(data, StringToFloat(DATA_INSERTED));

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
  ASSERT_EQ(pk, StringToFloat(PK_INSERTED));
  ASSERT_EQ(data_len, BYTES_EXPECTED);
  ASSERT_EQ(data, StringToFloat(DATA_INSERTED));

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Real, Arithmetic_Operators) {
  const int BYTES_EXPECTED = 4;

  float pk;
  float data;
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector <string> inserted_pk = {
    "-0.123456",
    "0.123456789",
    "1E+3"
  };

  vector <string> inserted_data = {
    "4",
    "9",
    "10.0"
  };

  vector <string> operations_query = {
    COL1_NAME + "+" + COL2_NAME,
    COL1_NAME + "-" + COL2_NAME,
    COL1_NAME + "/" + COL2_NAME,
    COL1_NAME + "*" + COL2_NAME,

    COL1_NAME + "^" + COL2_NAME,  // Power
    "|/" + COL2_NAME,             // Square Root
    "||/" + COL2_NAME,            // Cube Root
    "@" + COL2_NAME,              // Absolute Value
  };

  vector<vector<float>>expected_results = {};

  // initialization of expected_results
  for (int i = 0; i < inserted_data.size(); i++) {
    expected_results.push_back({});

    expected_results[i].push_back(StringToFloat(inserted_pk[i]) + StringToFloat(inserted_data[i]));
    expected_results[i].push_back(StringToFloat(inserted_pk[i]) - StringToFloat(inserted_data[i]));
    expected_results[i].push_back(StringToFloat(inserted_pk[i]) / StringToFloat(inserted_data[i]));
    expected_results[i].push_back(StringToFloat(inserted_pk[i]) * StringToFloat(inserted_data[i]));

    expected_results[i].push_back(pow(StringToFloat(inserted_pk[i]), StringToFloat(inserted_data[i])));
    expected_results[i].push_back(sqrt(StringToFloat(inserted_data[i])));
    expected_results[i].push_back(cbrt(StringToFloat(inserted_data[i])));
    expected_results[i].push_back(abs(StringToFloat(inserted_data[i])));
  }

  float col_results[operations_query.size()];
  SQLLEN col_len[operations_query.size()];
  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {};

  // initialization for bind_columns
  for (int i = 0; i < operations_query.size(); i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i + 1, SQL_C_FLOAT, (SQLPOINTER) &col_results[i], 0, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string{}; 
  string comma{};
  
  // insert_string initialization
  for (int i = 0; i < inserted_data.size(); i++) {
    insert_string += comma + "(" + inserted_pk[i] + "," + inserted_data[i] + ")";
    comma = ",";
  }

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));
 
  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, inserted_data.size());

  // Make sure inserted values are correct and operations
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, operations_query, vector<string> {}) + " ORDER BY " + COL1_NAME);
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(bind_columns));

  for (int i = 0; i < inserted_data.size(); i++) {
    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_SUCCESS);

    for (int j = 0; j < operations_query.size(); j++) {
      ASSERT_EQ(col_len[j], BYTES_EXPECTED);
      ASSERT_EQ(col_results[j], expected_results[i][j]);
    }
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Real, View_Creation) {
  const string VIEW_QUERY = "SELECT * FROM " + TABLE_NAME;
  const int BYTES_EXPECTED = 4;

  float pk;
  float data;
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector <string> valid_inserted_values = {
    "0",
    "1",
    "NULL"
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {
    {1, SQL_C_FLOAT, &pk, 0, &pk_len},
    {2, SQL_C_FLOAT, &data, 0,  &data_len}
  };

  string insert_string{}; 
  string comma{};
  
  for (int i = 0; i < valid_inserted_values.size(); i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + valid_inserted_values[i] + ")";
    comma = ",";
  }

  // Create Table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
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

  for (int i = 0; i < valid_inserted_values.size(); i++) {
    
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, BYTES_EXPECTED);
    ASSERT_EQ(pk, i);

    if (valid_inserted_values[i] != "NULL")
    {
      ASSERT_EQ(data_len, BYTES_EXPECTED);
      ASSERT_EQ(data, StringToFloat(valid_inserted_values[i]));
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
