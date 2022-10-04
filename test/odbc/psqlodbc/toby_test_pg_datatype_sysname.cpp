#include <gtest/gtest.h>
#include <sqlext.h>
#include "../src/drivers.h"
#include "../src/odbc_handler.h"
#include "../src/query_generator.h"

using std::pair;

const string TABLE_NAME = "master_dbo.sysname_table_odbc_test";
const string VIEW_NAME = "master_dbo.sysname_view_odbc_test";
const string DATATYPE = "sys.SYSNAME";
const int NUM_COLS = 4;
const string COL_NAMES[NUM_COLS] = {"pk", "sysname_1", "sysname_128", "sysname_20"  };
const int COL_LENGTH[NUM_COLS] = {10, 1, 128, 20};

const string COL_TYPES[NUM_COLS] = {
  DATATYPE + "(" + std::to_string(COL_LENGTH[0]) + ")",
  DATATYPE + "(" + std::to_string(COL_LENGTH[1]) + ")",
  DATATYPE + "(" + std::to_string(COL_LENGTH[2]) + ")",
  DATATYPE + "(" + std::to_string(COL_LENGTH[3]) + ")"
};

vector<pair<string, string>> TABLE_COLUMNS_SYSNAME = {
  {COL_NAMES[0], DATATYPE + " PRIMARY KEY"},
  {COL_NAMES[1], DATATYPE},
  {COL_NAMES[2], DATATYPE},
  {COL_NAMES[3], DATATYPE}
};

const string STRING_128 = "TQR6vCl9UH5qg2UEJMleJaa3yToVaUbhhxQ7e0SgHjrKg1TYvyUzTrLlO64uPEj572WjgLK6X5muDjK64tcWBr4bBp8hjnV"
  "ftzfLIYFEFCK0nAIuGhnjHIiB8Qc3ywbK";

const string STRING_1 = "a";
const string STRING_20 = "0123456789abcdefghij";

class PSQL_DataTypes_SYSNAME : public testing::Test{

  void SetUp() override {

    OdbcHandler test_setup(Drivers::GetDriver(ServerType::PSQL));
    test_setup.ConnectAndExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
  }

  void TearDown() override {

    OdbcHandler test_cleanup(Drivers::GetDriver(ServerType::PSQL));
    test_cleanup.ConnectAndExecQuery(DropObjectStatement("VIEW", VIEW_NAME));
    test_cleanup.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
  }
};

// helper function to initialize insert string (1, "", "", ""), etc.
string InitializeInsertString_sysname(const vector<vector<string>> &inserted_values) {

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

TEST_F(PSQL_DataTypes_SYSNAME, ColAttributes) {

  const int LENGTH_EXPECTED = 128;
  const int PRECISION_EXPECTED = 0;
  const int SCALE_EXPECTED = 0;
  const string NAME_EXPECTED = "unknown";
  const string PREFIX_EXPECTED = "'";
  const string SUFFIX_EXPECTED = "'";

  const int BUFFER_SIZE = 128;
  char name[BUFFER_SIZE];
  char suffix[BUFFER_SIZE];
  char prefix[BUFFER_SIZE];
  SQLLEN length;
  SQLLEN precision;
  SQLLEN scale;
  SQLLEN size;
  SQLLEN is_case_sensitive;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  // Create a table with columns defined with the specific datatype being tested. 
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_SYSNAME));
  odbcHandler.CloseStmt();

  // Select * From Table to ensure that it exists
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));

  for (int i = 1; i <= NUM_COLS; i++) {
    std::cout<<"The current string is: "<<i<<"\n";
    // Make sure column attributes are correct
    rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            i,
                            SQL_DESC_LENGTH, // Get the length of the column (size of char in columns)
                            NULL,
                            0,
                            NULL,
                            (SQLLEN*) &length);
    ASSERT_EQ(rcode, SQL_SUCCESS);
    std::cout<<"The current column Length: "<< length<<'\n';
    ASSERT_EQ(length, LENGTH_EXPECTED);
    
    rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            i,
                            SQL_DESC_PRECISION, // Get the precision of the column
                            NULL,
                            0,
                            NULL,
                            (SQLLEN*) &precision); 
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(precision, PRECISION_EXPECTED);

    rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            i,
                            SQL_DESC_SCALE, // Get the scale of the column
                            NULL,
                            0,
                            NULL,
                            (SQLLEN*) &scale); 
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(scale, SCALE_EXPECTED);

    rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            i,
                            SQL_DESC_TYPE_NAME, // Get the type name of the column
                            name,
                            BUFFER_SIZE,
                            NULL,
                            NULL);
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(string(name), NAME_EXPECTED);

    rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            i,
                            SQL_DESC_CASE_SENSITIVE, // Get the scale of the column
                            NULL,
                            0,
                            NULL,
                            (SQLLEN*) &is_case_sensitive); 
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(is_case_sensitive, SQL_FALSE);

    rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            i,
                            SQL_DESC_LITERAL_PREFIX, // Get the prefix of the column
                            name,
                            BUFFER_SIZE,
                            NULL,
                            NULL); 
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(string(name), PREFIX_EXPECTED);

    rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            i,
                            SQL_DESC_LITERAL_SUFFIX, // Get the suffix character of the column
                            name,
                            BUFFER_SIZE,
                            NULL,
                            NULL);
    ASSERT_EQ(rcode, SQL_SUCCESS); 
    ASSERT_EQ(string(name), SUFFIX_EXPECTED);
  }

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_SYSNAME, Table_Create_Fail) {

  vector<vector<pair<string, string>>> invalid_columns{
    {{"invalid1", DATATYPE + "(-1)"}},
    {{"invalid2", DATATYPE + "(0)"}},
    // {{"invalid3", DATATYPE + "(8001)"}}, -- This works on the postgres endpoint?
    {{"invalid4", DATATYPE + "(NULL)"}}
  };

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

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

TEST_F(PSQL_DataTypes_SYSNAME, Insertion_Success) {

  const int BUFFER_LENGTH = 8192;

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  vector<vector<string>> inserted_values = {
    {"1", "", "", "" }, // empty strings
    {"2", STRING_1, STRING_128, STRING_20}, // max values
    {"3", "a", "def", "ghi"}, // regular values
    {"4", "NULL", "NULL", "NULL"} // NULL values
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString_sysname(inserted_values);

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_SYSNAME));
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

      if (inserted_values[i][j] != "NULL") {
        ASSERT_EQ(string(col_results[j]), inserted_values[i][j]);
        ASSERT_EQ(col_len[j], inserted_values[i][j].size());
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

TEST_F(PSQL_DataTypes_SYSNAME, Insertion_Failure) {

  const int BUFFER_LENGTH = 8192;

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  vector<vector<string>> inserted_values = {
    {"2", "", STRING_128 + "t", ""}, // second col exceeds by 1 char
  };

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_SYSNAME));
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

TEST_F(PSQL_DataTypes_SYSNAME, Update_Success) {

  const int BUFFER_LENGTH = 8192;
  const int AFFECTED_ROWS_EXPECTED = 1;
  const string PK_VAL = "1";

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  vector<vector<string>> inserted_values = {
    {PK_VAL, "1", "2", "3"} 
  };

  vector<vector<string>> updated_values = {
    {PK_VAL, "a", "b", "c"}, // standard values
    {PK_VAL, STRING_1, STRING_128, STRING_20}, // max values
    {PK_VAL, "", "", ""} // min values
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString_sysname(inserted_values);


  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_SYSNAME));
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
      
      ASSERT_EQ(string(col_results[j]), updated_values[i][j]);
      ASSERT_EQ(col_len[j], updated_values[i][j].size());
    }

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_NO_DATA);
    odbcHandler.CloseStmt();
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_SYSNAME, Update_Fail) {

  const int BUFFER_LENGTH = 8192;
  const int AFFECTED_ROWS_EXPECTED = 1;
  const string PK_VAL = "1";

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  vector<vector<string>> inserted_values = {
    {PK_VAL, "1", "2", "3"} 
  };

  vector<vector<string>> updated_values = {
    {PK_VAL, STRING_1 + "1", STRING_128 + "1", STRING_20 + "1"} // max values + 1 char
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString_sysname(inserted_values);

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_SYSNAME));
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

  vector<pair<string,string>> update_col;

  // setup update column
  for (int j = 0; j <NUM_COLS; j++) {
    string value = string("'") + updated_values[0][j] + string("'");
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
  for (int i = 0; i < NUM_COLS; i++) {
    ASSERT_EQ(string(col_results[i]), inserted_values[0][i]);
    ASSERT_EQ(col_len[i], inserted_values[0][i].size());
  }

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_SYSNAME, String_Operators) {
  const int BUFFER_LENGTH=256;
  const int BYTES_EXPECTED = 4;
  const int DOUBLE_BYTES_EXPECTE = 8;
  int pk;
  char data[BUFFER_LENGTH];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

const int NUM_COLS = 2;
const string COL_NAMES[NUM_COLS] = {"pk", "data"};
const int COL_LENGTH[NUM_COLS] = {128, 128};

const string COL_TYPES[NUM_COLS] = {
  DATATYPE,
  DATATYPE
};

vector<pair<string, string>> TABLE_COLUMNS_NTEXT = {
  {COL_NAMES[0], COL_TYPES[0] + " PRIMARY KEY"},
  {COL_NAMES[1], COL_TYPES[1]}
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
    COL_NAMES[0]+ "||" + COL_NAMES[1],
    "lower("+COL_NAMES[1]+")"
  };


  vector<vector<string>>expected_results = {{},{}};

  // initialization of expected_results
  for (int i = 0; i < inserted_pk.size(); i++) {
    expected_results[i].push_back(inserted_pk[i] + inserted_data[i]);
    string current=inserted_data[i];
    transform(current.begin(), current.end(), current.begin(), ::tolower);
    expected_results[i].push_back(current);
    
  }

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns = {};

  // initialization for bind_columns
  for (int i = 0; i < operations_query.size(); i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
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
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_NTEXT));
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
    odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, operations_query, vector<string> {}, COL_NAMES[0] + "=" + "'"+inserted_pk[i]+"'"));
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
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}


TEST_F(PSQL_DataTypes_SYSNAME, View_creation) {

  const string VIEW_QUERY = "SELECT * FROM " + TABLE_NAME;
  const int BUFFER_LENGTH = 8192;

  char col_results[NUM_COLS][BUFFER_LENGTH];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  vector<vector<string>> inserted_values = {
    {"1", "", "", "" }, // empty strings
    {"2", STRING_1, STRING_128, STRING_20}, // max values
    {"3", "a", "def", "ghi"}, // regular values
    {"4", "NULL", "NULL", "NULL"} // NULL values
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_CHAR, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
    bind_columns.push_back(tuple_to_insert);
  }

  string insert_string = InitializeInsertString_sysname(inserted_values);

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS_SYSNAME));
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

        ASSERT_EQ(string(col_results[j]), inserted_values[i][j]);
        ASSERT_EQ(col_len[j], inserted_values[i][j].size());
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

TEST_F(PSQL_DataTypes_SYSNAME, Table_Composite_Keys) {
  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL_NAMES[0], "int" },
    {COL_NAMES[1], DATATYPE}
  };
  const string PKTABLE_NAME = TABLE_NAME.substr(TABLE_NAME.find('.') + 1, TABLE_NAME.length());
  const string SCHEMA_NAME = TABLE_NAME.substr(0, TABLE_NAME.find('.'));

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
  const int BUFFER_LENGTH=256;
  int pk;
  char data[BUFFER_LENGTH];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

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

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS, table_constraints));
  odbcHandler.CloseStmt();

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
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string>{COL_NAMES[0]}));

  // Make sure inserted values are correct
  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_LENGTH, &data_len}
  };

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

  // Attempt to insert values that violates composite constraint and assert that they all fail
  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

  rcode = SQLExecDirect(odbcHandler.GetStatementHandle(), (SQLCHAR *)InsertStatement(TABLE_NAME, insert_string).c_str(), SQL_NTS);
  ASSERT_EQ(rcode, SQL_ERROR);

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}


TEST_F(PSQL_DataTypes_SYSNAME, Table_Unique_Constraints) {
  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL_NAMES[0], "INT PRIMARY KEY" },
    {COL_NAMES[1], DATATYPE + " UNIQUE"}
  };
 
 
  const string UNIQUE_COLUMN_NAME = COL_NAMES[1];

  const int PK_BYTES_EXPECTED = 4;
  const int BUFFER_LENGTH=256;
  int pk;
  char data[BUFFER_LENGTH];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

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

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Check if unique constraint still matches after creation
  char column_name[BUFFER_LENGTH];
  char type_name[BUFFER_LENGTH];

  vector<tuple<int, int, SQLPOINTER, int>> table_BIND_COLUMNS = {
    {1, SQL_C_CHAR, column_name, BUFFER_LENGTH},
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
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string>{COL_NAMES[1]}));

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

    rcode = SQLExecDirect(odbcHandler.GetStatementHandle(), (SQLCHAR *)InsertStatement(TABLE_NAME, insert_string).c_str(), SQL_NTS);
    ASSERT_EQ(rcode, SQL_ERROR);
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}