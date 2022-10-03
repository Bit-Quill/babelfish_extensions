#include <gtest/gtest.h>
#include <sqlext.h>
#include "odbc_handler.h"
#include "query_generator.h"
#include <string>
#include <math.h>

using std::pair;

const string TABLE_NAME = "master_dbo.float_table_odbc_test";
const string VIEW_NAME = "master_dbo.float_view_odbc_test";
const string DATATYPE = "sys.float";
const int NUM_COLS = 2;
const string COL_NAMES[NUM_COLS] = {"pk", "float"};
const int COL_LENGTH[NUM_COLS]={10,308};
const int COL_SCALE[NUM_COLS] = {0, 308};

// unsure of how these values are calculated, but they match with postgres' (non-sys) decimal type
// Postgres documentation just says they're bytes size are variable

const string COL_TYPES[NUM_COLS] = {
  DATATYPE ,
  DATATYPE 
};

vector<pair<string, string>> TABLE_COLUMNS = {
  {COL_NAMES[0], COL_TYPES[0] + " PRIMARY KEY"},
  {COL_NAMES[1], COL_TYPES[1]}
};  
double StringToDouble(const string &value){
    return atof(value.c_str());
  }

const string float_15 = "1e308";
// const string float_383 = "0.12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123";
const string float_383 ="-1e307";
class PSQL_DataTypes_Float : public testing::Test{

  void SetUp() override {

    OdbcHandler test_setup;
    test_setup.ConnectAndExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
  }
  

  void TearDown() override {

    OdbcHandler test_cleanup;
    test_cleanup.ConnectAndExecQuery(DropObjectStatement("VIEW", VIEW_NAME));
    test_cleanup.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
  }
};

// pads 0 at the end of the results depending on the scale number
// double FormatDecWithScale(string decimal, const int &scale) {

//   size_t dec_pos = decimal.find('.');

//   if (dec_pos == std::string::npos) {
//     if (scale == 0) // if no decimal sign and scale is 0, no need to append
//       return atof(decimal);
//     dec_pos = decimal.size();
//     decimal += ".";
//   }

//   // add extra 0s
//   int zeros_needed = scale - (decimal.size() - dec_pos - 1);
//   for (int i = 0; i < zeros_needed; i++) {
//     decimal += "0";
//   }

//   return atof(decimal);
// }

// helper function to initialize insert string (1, "", "", ""), etc.
string InitializeInsertString(const vector<vector<string>> &inserted_values) {

  string insert_string{};
  string comma{};

  for (int i = 0; i< inserted_values.size(); ++i) {

    insert_string += comma + "(";
    string comma2{};

    for (int j = 0; j < NUM_COLS; j++) {
      insert_string += comma2 + inserted_values[i][j];
      comma2 = ",";
    }

    insert_string += ")";
    comma = ",";
  }
  return insert_string;
}

TEST_F(PSQL_DataTypes_Float, ColAttributes) {

  const string NAME_EXPECTED = "float8";
  const int Lenghth_expected[NUM_COLS]={8,17};
  const int PREICSION_EXPECTED=0;
  const int SCALE_EXPECTED=0;
  const int COL_LENGTH_EXPECTED[NUM_COLS]={24,24};
  const int BUFFER_SIZE = 256;
  char name[BUFFER_SIZE];
  SQLLEN length;
  SQLLEN precision;
  SQLLEN scale;
  SQLLEN display_size;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  // Create a table with columns defined with the specific datatype being tested. 
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Select * From Table to ensure that it exists
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string> {COL_NAMES[0]}));
  for (int i = 1; i <= NUM_COLS; i++) {
    std::cout<<"Current Column: "<< i-1<<'\n';
    // Make sure column attributes are correct
    rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            i,
                            SQL_DESC_LENGTH, // Get the length of the column
                            NULL,
                            0,
                            NULL,
                            (SQLLEN*) &length);
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(length, Lenghth_expected[i-1]);

    rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            i,
                            SQL_DESC_DISPLAY_SIZE, // Get the display size of the column (size of char in columns)
                            NULL,
                            0,
                            NULL,
                            (SQLLEN*) &display_size);
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(display_size, COL_LENGTH_EXPECTED[i-1]); // add 2 since we also add the decimal and negative characters
    
    rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                            i,
                            SQL_DESC_PRECISION, // Get the precision of the column
                            NULL,
                            0,
                            NULL,
                            (SQLLEN*) &precision); 
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(precision, PREICSION_EXPECTED);

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

  }

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);
}

TEST_F(PSQL_DataTypes_Float, Table_Create_Fail) {

  vector<vector<pair<string, string>>> invalid_columns{
    {{"invalid1", DATATYPE + "(0)"}}, // must have precision of 1 or greater
    {{"invalid2", DATATYPE + "(11)"}} // scale cannot be larger than precision
    // Does not work on Postgres endpoint
    // , {{"invalid4", DATATYPE + "(39, 39)"}} // max precision is 38 
  };

  RETCODE rcode;
  OdbcHandler odbcHandler;

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

TEST_F(PSQL_DataTypes_Float, Insertion_Success) {

  const int BUFFER_LENGTH = 8192;
  const int BYTES_EXPECTED=8;
  double col_results[NUM_COLS];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector<vector<string>> inserted_values = {
    {"1", float_15 }, // smallest numbers
    {"2", float_383}, // max values
    {"3",  "0.4347509234", }, // random regular values
    {"4", "NULL"} // NULL values
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_DOUBLE, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
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

      if (inserted_values[i][j] != "NULL") {
        std::cout<<"insert value is "<<col_results[j]<<'\n';
        ASSERT_EQ(col_results[j], StringToDouble(inserted_values[i][j]));
        // std::cout<<"length is "<<(col_len[j])<<'\n';
        ASSERT_EQ(col_len[j], BYTES_EXPECTED);
      
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

TEST_F(PSQL_DataTypes_Float, Insertion_Failure) {


  SQLLEN col_len[NUM_COLS];

  RETCODE rcode;
  OdbcHandler odbcHandler; 

  vector<vector<string>> inserted_values = {
    {"0","1e309"},
    {"1", "-1e309"},
    {"2", "1e-325"},
    {"3", "-1e-324"}
     // 309 would succeed the failure   range  -1e308 to -1e-307  0 and 0 1e307 to 1e308     
  };

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Insert invalid values in table and assert error
  for (int i = 0; i < inserted_values.size(); i++) {

    string insert_string = "(";
    string comma{};

    // create insert_string (1, ..., ..., ...)
    for (int j = 0; j < NUM_COLS; j++) {
      insert_string += comma + inserted_values[i][j];
      comma = ",";
    }
    insert_string += ")";
    // std::cout<<"Current Value is: "<< insert_string;
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

TEST_F(PSQL_DataTypes_Float, Update_Success) {

  const int BUFFER_LENGTH = 8192;
  const int AFFECTED_ROWS_EXPECTED = 1;
  const int BYTES_EXPECTED = 8;
  const string PK_VAL = "1";

  double col_results[NUM_COLS];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector<vector<string>> inserted_values = {
    {PK_VAL, "0.1"} 
  };

  vector<vector<string>> updated_values = {
    // standard values
    {PK_VAL, float_383}, // max values
    {PK_VAL, "0"},
    {PK_VAL,float_15} // min values
    
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_DOUBLE, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
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

      ASSERT_EQ(col_results[j],StringToDouble(inserted_values[i][j]));
      ASSERT_EQ(col_len[j], BYTES_EXPECTED);
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
      
      ASSERT_EQ(col_results[j], StringToDouble(updated_values[i][j]));
      ASSERT_EQ(col_len[j], BYTES_EXPECTED); 
    }

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_NO_DATA);
    odbcHandler.CloseStmt();
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Float, Update_Fail) {
  const int BYTES_EXPECETED=8;
  const int BUFFER_LENGTH = 8192;
  const int AFFECTED_ROWS_EXPECTED = 1;
  const string PK_VAL = "1";

  double col_results[NUM_COLS];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector<vector<string>> inserted_values = {
    {PK_VAL, "0.1"} 
  };

  vector<vector<string>> updated_values = {
    {"1", "1e400"} // first col exceeds by 1 digit
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_DOUBLE, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
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
      ASSERT_EQ(col_results[j], StringToDouble(inserted_values[i][j]));
      ASSERT_EQ(col_len[j], BYTES_EXPECETED);
    }
  }

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  for (int i = 0; i < inserted_values.size(); i++) 
  {
    odbcHandler.CloseStmt();
    vector<pair<string,string>> update_col;

    // setup update column
    for (int j = 0; j <NUM_COLS; j++) {
      update_col.push_back(pair<string,string>(COL_NAMES[j], updated_values[i][j]));
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
      ASSERT_EQ(col_results[j], StringToDouble(inserted_values[i][j]));
      ASSERT_EQ(col_len[j], BYTES_EXPECETED);
    }
  }

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Float,Arithmetic_Operators) {
  const int BUFFER_LENGTH=8192;
  const int BYTES_EXPECTED = 8;

  unsigned char pk;
  unsigned char data[BUFFER_LENGTH];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector <string> inserted_pk = {
    "8"
  };

  vector <string> inserted_data = {
    "2"
  };

  vector <string> operations_query = {
    COL_NAMES[0] + "+" + COL_NAMES[1],
    COL_NAMES[0] + "-" + COL_NAMES[1],
    COL_NAMES[0] + "*" + COL_NAMES[1],
    COL_NAMES[0] + "/" + COL_NAMES[1],
    "ABS("+COL_NAMES[0]+")",
    "POWER("+COL_NAMES[0]+","+COL_NAMES[1]+")",
    "||/ "+COL_NAMES[0],
    "LOG("+COL_NAMES[0]+")"    
  };

  vector<vector<string>>expected_results = {{},{}};

  // initialization of expected_results
  for (int i = 0; i < inserted_pk.size(); i++) {
    expected_results[i].push_back("10");
    expected_results[i].push_back("6");
    expected_results[i].push_back("16");
    expected_results[i].push_back("4");
    expected_results[i].push_back("8");
    expected_results[i].push_back("64");
    expected_results[i].push_back("2");
    expected_results[i].push_back("0.9030899869919435");
  }

  char col_results[operations_query.size()][BUFFER_LENGTH];
  SQLLEN col_len[operations_query.size()];
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

  for (int i = 0; i < inserted_data.size(); ++i) {
    
    odbcHandler.CloseStmt();
    odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, operations_query, vector<string> {}, COL_NAMES[0] + "=" + inserted_pk[i]));
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



TEST_F(PSQL_DataTypes_Float, View_creation) {
  const int BYTES_EXPECTED = 8;
  const string VIEW_QUERY = "SELECT * FROM " + TABLE_NAME;
  const int BUFFER_LENGTH = 8192;

  double col_results[NUM_COLS];
  SQLLEN col_len[NUM_COLS];
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler;

  vector<vector<string>> inserted_values = {
    {"1", float_15}, // smallest numbers
    {"2", float_383}, // max values
    {"3", "0.4347509234"}, // random regular values
    {"4",  "NULL"} // NULL values
  };

  vector<tuple<int, int, SQLPOINTER, int, SQLLEN* >> bind_columns{};

  // initialize bind_columns
  for (int i = 0; i < NUM_COLS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN*> tuple_to_insert(i+1, SQL_C_DOUBLE, (SQLPOINTER) &col_results[i], BUFFER_LENGTH, &col_len[i]);
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

        ASSERT_EQ(col_results[j], StringToDouble(inserted_values[i][j]));
        ASSERT_EQ(col_len[j], BYTES_EXPECTED);
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


TEST_F(PSQL_DataTypes_Float, Table_Composite_Keys) {
  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL_NAMES[0], DATATYPE},
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
  }
  table_constraints += ")";

  const int DATA_BYTES_EXPECTED = 8;
  const int BUFFER_SIZE=256;
  const int PK_BYTES_EXPECTED=4;

  int pk;
  double data;
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
    {5, SQL_C_ULONG, &key_sq, BUFFER_SIZE},
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
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string>{COL_NAMES[0]}));

  // Make sure inserted values are correct
  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_ULONG, &pk, 0, &pk_len},
    {2, SQL_C_DOUBLE, &data, 0, &data_len}
  };

  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(BIND_COLUMNS));
  


  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, PK_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);
    if (VALID_INSERTED_VALUES[i] != "NULL") {
      ASSERT_EQ(data_len, DATA_BYTES_EXPECTED);
      ASSERT_EQ(data, StringToDouble(VALID_INSERTED_VALUES[i]));
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


TEST_F(PSQL_DataTypes_Float, Table_Unique_Constraints) {
  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL_NAMES[0], "INT PRIMARY KEY"},
    {COL_NAMES[1], DATATYPE+ " UNIQUE"}
  };
  const string UNIQUE_COLUMN_NAME = COL_NAMES[1];

  const int PK_BYTES_EXPECTED = 4;
  const int DATA_BYTES_EXPECTED = 8;
  const int BUFFER_SIZE=256;

  int pk;
  double data;
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
    {2, SQL_C_DOUBLE, &data, 0, &data_len}
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
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string>{COL_NAMES[0]}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(BIND_COLUMNS));

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, PK_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);
    if (VALID_INSERTED_VALUES[i] != "NULL") {
      ASSERT_EQ(data_len, DATA_BYTES_EXPECTED);
      ASSERT_EQ(data, StringToDouble(VALID_INSERTED_VALUES[i]));
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