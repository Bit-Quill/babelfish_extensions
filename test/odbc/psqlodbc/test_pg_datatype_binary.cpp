#include <gtest/gtest.h>
#include <sqlext.h>
#include "../src/odbc_handler.h"
#include "../src/query_generator.h"
#include "../src/drivers.h"
#include <iostream>
#include <iomanip>
using std::pair;

const string TABLE_NAME = "master_dbo.binary_table_odbc_test";
const string COL1_NAME = "pk";
const string COL2_NAME = "data";
const string DATATYPE_NAME = "sys.binary";
// Default, n = 1, but PG n=4, BBF n=1
const int DATATYPE_SIZE = 1;
const string VIEW_NAME = "master_dbo.binary_view_odbc_test";
const vector<pair<string, string>> TABLE_COLUMNS = {
  {COL1_NAME, "INT PRIMARY KEY"},
  {COL2_NAME, DATATYPE_NAME + "(" + std::to_string(DATATYPE_SIZE) + ")"}
};
const int DATA_COLUMN = 2;
const int BUFFER_SIZE = 256;
const int INT_BYTES_EXPECTED = 4;
// Bytes expected = (2 * n) + 2
// 1 byte takes 2 chracters (in hex) + prepend `0x`
// NOTE - BBF does not prepend `0x` to return data, whilst PG does
const int BINARY_BYTES_EXPECTED = (2 * DATATYPE_SIZE) + 2;

class PSQL_DataTypes_Binary : public testing::Test {
  void SetUp() override {
    if (!Drivers::DriverExists(ServerType::PSQL)) {
      GTEST_SKIP() << "PSQL Driver not present: skipping all tests for this fixture.";
    }

    OdbcHandler test_setup(Drivers::GetDriver(ServerType::PSQL));
    test_setup.ConnectAndExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
  }

  void TearDown() override {
    OdbcHandler test_teardown(Drivers::GetDriver(ServerType::PSQL));
    test_teardown.ConnectAndExecQuery(DropObjectStatement("VIEW", VIEW_NAME));
    test_teardown.CloseStmt();
    test_teardown.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
  }
};

std::string GetHexRepresentation(std::string inserted_int) {
  std::stringstream stream;
  stream << std::hex << strtoul(inserted_int.c_str(), nullptr, 10);
  return stream.str();
}

// Helper to compare inserted integer string with returned hexadecimal strings
bool BinaryCompare(std::string actual_hex, std::string expected_hex, size_t expected_size) {
  const size_t ACTUAL_LENGTH = actual_hex.length();
  const size_t EXPECTED_LENGTH = expected_hex.length();
  for (size_t i = 0; i < expected_size; i++) {
    if (i >= EXPECTED_LENGTH) {
      if (actual_hex[ACTUAL_LENGTH - 1 - i] != '0') {
        return false;
      }
    }
    else if (actual_hex[ACTUAL_LENGTH - 1 - i] != expected_hex[EXPECTED_LENGTH - 1 - i]) {
      return false;
    }
  }
  return true;
}

TEST_F(PSQL_DataTypes_Binary, Table_Creation) {
  // TODO - Expected needs to be fixed.
  const int LENGTH_EXPECTED = 1;
  const int PRECISION_EXPECTED = 0;
  const int SCALE_EXPECTED = 0;
  const string NAME_EXPECTED = "unknown";

  char name[BUFFER_SIZE];
  SQLLEN length;
  SQLLEN precision;
  SQLLEN scale;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  // Create a table with columns defined with the specific datatype being tested.
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Select * From Table to ensure that it exists
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string>{COL1_NAME}));

  // Make sure column attributes are correct
  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
                          SQL_DESC_LENGTH, // Get the length of the column (size of char in columns)
                          NULL,
                          0,
                          NULL,
                          (SQLLEN *)&length);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(length, LENGTH_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
                          SQL_DESC_PRECISION, // Get the precision of the column
                          NULL,
                          0,
                          NULL,
                          (SQLLEN *)&precision);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(precision, PRECISION_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
                          SQL_DESC_SCALE, // Get the scale of the column
                          NULL,
                          0,
                          NULL,
                          (SQLLEN *)&scale);
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

TEST_F(PSQL_DataTypes_Binary, Insertion_Success) {
  int pk;
  char data[BUFFER_SIZE];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  const vector<string> VALID_INSERTED_VALUES = {
    "NULL",
    "00",     // Min
    "0",      // Min, different format
    "46",     // Rand
    "02",     // Rand, different format
    "255",    // Max
    "256"     // Max + 1, truncate
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_SIZE, &data_len}
  };

  string insert_string{};
  string comma{};

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
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
    ASSERT_EQ(pk_len, INT_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);    
    if (VALID_INSERTED_VALUES[i] != "NULL") {
      std::string expected_hex = GetHexRepresentation(VALID_INSERTED_VALUES[i]);
      ASSERT_EQ(data_len, BINARY_BYTES_EXPECTED);
      ASSERT_TRUE(BinaryCompare(data, expected_hex, BINARY_BYTES_EXPECTED - 2));
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

TEST_F(PSQL_DataTypes_Binary, Update_Success) {
  const string PK_INSERTED = "0";
  const string DATA_INSERTED = "123";

  const vector <string> DATA_UPDATED_VALUES = {
    "NULL",
    "00",     // Min
    "0",      // Min, different format
    "46",     // Rand
    "02",     // Rand, different format
    "255",    // Max
    "256"     // Max + 1, truncate
  };
  const int NUM_OF_DATA = DATA_UPDATED_VALUES.size();

  const string INSERT_STRING = "(" + PK_INSERTED + "," + DATA_INSERTED + ")";
  const string UPDATE_WHERE_CLAUSE = COL1_NAME + " = " + PK_INSERTED;

  const int AFFECTED_ROWS_EXPECTED = 1;

  int pk;
  char data[BUFFER_SIZE];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_SIZE, &data_len}
  };

  vector<pair<string, string>> update_col{};

  for (int i = 0; i < NUM_OF_DATA; i++) {
    update_col.push_back(pair<string, string>(COL2_NAME, DATA_UPDATED_VALUES[i]));
  }

  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Insert valid values into the table using the correct ODBC data type mapping.
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, INSERT_STRING));
  odbcHandler.CloseStmt();

  // Bind Columns
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(BIND_COLUMNS));

  // Assert that value is inserted properly
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string>{COL1_NAME}));
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(pk_len, INT_BYTES_EXPECTED);
  ASSERT_EQ(pk, atoi(PK_INSERTED.c_str()));
  std::string expected_hex = GetHexRepresentation(DATA_INSERTED);
  ASSERT_EQ(data_len, BINARY_BYTES_EXPECTED);
  ASSERT_TRUE(BinaryCompare(data, expected_hex, BINARY_BYTES_EXPECTED - 2));

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);
  odbcHandler.CloseStmt();

  // Update value multiple times
  for (int i = 0; i < NUM_OF_DATA; i++) {
    odbcHandler.ExecQuery(UpdateTableStatement(TABLE_NAME, vector<pair<string, string>>{update_col[i]}, UPDATE_WHERE_CLAUSE));

    rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(affected_rows, AFFECTED_ROWS_EXPECTED);

    odbcHandler.CloseStmt();

    // Assert that updated value is present
    odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string>{COL1_NAME}));
    rcode = SQLFetch(odbcHandler.GetStatementHandle());

    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, INT_BYTES_EXPECTED);
    ASSERT_EQ(pk, atoi(PK_INSERTED.c_str()));
    if (DATA_UPDATED_VALUES[i] != "NULL") {
      std::string expected_hex = GetHexRepresentation(DATA_UPDATED_VALUES[i]);
      ASSERT_EQ(data_len, BINARY_BYTES_EXPECTED);
      ASSERT_TRUE(BinaryCompare(data, expected_hex, BINARY_BYTES_EXPECTED - 2));
    }
    else {
      ASSERT_EQ(data_len, SQL_NULL_DATA);
    }

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_NO_DATA);
    odbcHandler.CloseStmt();
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

// Disabled, Comparison operator `<`, not working as expected
//    0x00000000 < 0x00000000	returns true
//    0x00000060 < 0x000000ff returns false
// Explicit casting is used, ie OPERATOR(sys.=)
TEST_F(PSQL_DataTypes_Binary, DISABLED_Comparison_Operators) {
  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL1_NAME, DATATYPE_NAME + " PRIMARY KEY"},
    {COL2_NAME, DATATYPE_NAME}
  };

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));
  SQLLEN affected_rows;
  const int BYTES_EXPECTED = 1;

  const vector<string> INSERTED_PK = {
    "0",      // A = B
    "96",     // A < B
    "128",    // A > B
  };

  const vector<string> INSERTED_DATA = {
    "0",      // A = B
    "255",    // A < B
    "32",     // A > B
  };
  const int NUM_OF_DATA = INSERTED_DATA.size();

  vector<string> OPERATIONS_QUERY = {
    COL1_NAME + " OPERATOR(sys.=) " + COL2_NAME,
    COL1_NAME + " OPERATOR(sys.<>) " + COL2_NAME,
    COL1_NAME + " OPERATOR(sys.<) " + COL2_NAME,
    COL1_NAME + " OPERATOR(sys.<=) " + COL2_NAME,
    COL1_NAME + " OPERATOR(sys.>) " + COL2_NAME,
    COL1_NAME + " OPERATOR(sys.>=) " + COL2_NAME
  };
  const int NUM_OF_OPERATIONS = OPERATIONS_QUERY.size();

  // initialization of expected_results
  vector<vector<char>> expected_results = {};
  for (int i = 0; i < NUM_OF_DATA; i++) {
    expected_results.push_back({});
    const int DATA_1 = atoi(INSERTED_PK[i].c_str());
    const int DATA_2 = atoi(INSERTED_DATA[i].c_str());
    
    expected_results[i].push_back(DATA_1 == DATA_2 ? '1' : '0');
    expected_results[i].push_back(DATA_1 != DATA_2 ? '1' : '0');
    expected_results[i].push_back(DATA_1 < DATA_2 ? '1' : '0');
    expected_results[i].push_back(DATA_1 <= DATA_2 ? '1' : '0');
    expected_results[i].push_back(DATA_1 > DATA_2 ? '1' : '0');
    expected_results[i].push_back(DATA_1 >= DATA_2 ? '1' : '0');
  }

  char col_results[NUM_OF_OPERATIONS];
  SQLLEN col_len[NUM_OF_OPERATIONS];
  vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {};

  // initialization for bind_columns
  for (int i = 0; i < NUM_OF_OPERATIONS; i++) {
    tuple<int, int, SQLPOINTER, int, SQLLEN *> tuple_to_insert(i + 1, SQL_C_CHAR, (SQLPOINTER)&col_results[i], BUFFER_SIZE, &col_len[i]);
    BIND_COLUMNS.push_back(tuple_to_insert);
  }

  string insert_string{};
  string comma{};

  // insert_string initialization
  for (int i = 0; i < NUM_OF_DATA; i++) {
    insert_string += comma + "(" + INSERTED_PK[i] + "," + INSERTED_DATA[i] + ")";
    comma = ",";
  }

  // Create table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));

  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, NUM_OF_DATA);

  // Make sure inserted values are correct and operations
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(BIND_COLUMNS));

  for (int i = 0; i < NUM_OF_DATA; i++) {
    odbcHandler.CloseStmt();
    odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, OPERATIONS_QUERY, vector<string>{COL1_NAME}, COL1_NAME + " operator(sys.=) cast(" + INSERTED_PK[i] + "as sys.binary)"));
    ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(BIND_COLUMNS));

    rcode = SQLFetch(odbcHandler.GetStatementHandle());
    ASSERT_EQ(rcode, SQL_SUCCESS);
    
    for (int j = 0; j < NUM_OF_OPERATIONS; j++) {
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

TEST_F(PSQL_DataTypes_Binary, View_Creation) {
  const string VIEW_QUERY = "SELECT * FROM " + TABLE_NAME;

  int pk;
  char data[BUFFER_SIZE];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  const vector<string> VALID_INSERTED_VALUES = {
    "NULL",
    "00",     // Min
    "0",      // Min, different format
    "46",     // Rand
    "02",     // Rand, different format
    "255",    // Max
    "256"     // Max + 1, truncate
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_SIZE, &data_len}
  };

  string insert_string{};
  string comma{};

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

  // Create Table
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Insert valid values into the table and assert affected rows
  odbcHandler.ExecQuery(InsertStatement(TABLE_NAME, insert_string));

  rcode = SQLRowCount(odbcHandler.GetStatementHandle(), &affected_rows);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(affected_rows, NUM_OF_INSERTS);

  odbcHandler.CloseStmt();

  // Create view
  odbcHandler.ExecQuery(CreateViewStatement(VIEW_NAME, VIEW_QUERY));
  odbcHandler.CloseStmt();

  // Select all from the tables and assert that the following attributes of the type is correct:
  odbcHandler.ExecQuery(SelectStatement(VIEW_NAME, {"*"}, vector<string>{COL1_NAME}));

  // Make sure inserted values are correct
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(BIND_COLUMNS));

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, INT_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);

    if (VALID_INSERTED_VALUES[i] != "NULL") {
      std::string expected_hex = GetHexRepresentation(VALID_INSERTED_VALUES[i]);
      ASSERT_EQ(data_len, BINARY_BYTES_EXPECTED);
      ASSERT_TRUE(BinaryCompare(data, expected_hex, BINARY_BYTES_EXPECTED - 2));
    }
    else {
      ASSERT_EQ(data_len, SQL_NULL_DATA); 
    } 
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("VIEW", VIEW_NAME));

  odbcHandler.CloseStmt();
  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Binary, Table_Unique_Constraints) {
  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL1_NAME, "INT PRIMARY KEY"},
    {COL2_NAME, DATATYPE_NAME + + "(" + std::to_string(DATATYPE_SIZE) + ") UNIQUE"}
  };
  const string UNIQUE_COLUMN_NAME = COL2_NAME;

  int pk;
  char data[BUFFER_SIZE];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  const vector<string> VALID_INSERTED_VALUES = {
    "0",      // Min
    "46",     // Rand
    "02",     // Rand, different format
    "255"     // Max
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_SIZE, &data_len}
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

  vector<tuple<int, int, SQLPOINTER, int>> table_BIND_COLUMNS = {
      {1, SQL_C_CHAR, column_name, BUFFER_SIZE},
  };
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(table_BIND_COLUMNS));

  const string UNIQUE_KEY_QUERY =
    "SELECT C.COLUMN_NAME FROM "
    "INFORMATION_SCHEMA.TABLE_CONSTRAINTS T "
    "JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C "
    "ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME "
    "WHERE "
    "C.TABLE_NAME='" + TABLE_NAME.substr(TABLE_NAME.find('.') + 1, TABLE_NAME.length()) + "' "
    "AND T.CONSTRAINT_TYPE='UNIQUE'";
  odbcHandler.ExecQuery(UNIQUE_KEY_QUERY);
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
    ASSERT_EQ(pk_len, INT_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);
    std::string expected_hex = GetHexRepresentation(VALID_INSERTED_VALUES[i]);
    ASSERT_EQ(data_len, BINARY_BYTES_EXPECTED);
    ASSERT_TRUE(BinaryCompare(data, expected_hex, BINARY_BYTES_EXPECTED - 2));
  }

  // Assert that there is no more data
  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();

  // Attempt to insert values that violates unique constraint and assert that they all fail
  for (int i = 0; i < 2 * NUM_OF_INSERTS; i++) {
    string insert_string = "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i % NUM_OF_INSERTS] + ")";

    rcode = SQLExecDirect(odbcHandler.GetStatementHandle(), (SQLCHAR *)InsertStatement(TABLE_NAME, insert_string).c_str(), SQL_NTS);
    ASSERT_EQ(rcode, SQL_ERROR);
  }

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

TEST_F(PSQL_DataTypes_Binary, Table_Composite_Keys) {
  const vector<pair<string, string>> TABLE_COLUMNS = {
    {COL1_NAME, "INT"},
    {COL2_NAME, DATATYPE_NAME + "(" + std::to_string(DATATYPE_SIZE) + ")"}
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

  int pk;
  char data[BUFFER_SIZE];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  const vector<string> VALID_INSERTED_VALUES = {
    "00",     // Min
    "46",     // Rand
    "02",     // Rand, different format
    "255"     // Max
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_SIZE, &data_len}
  };

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

  vector<tuple<int, int, SQLPOINTER, int>> constraints_BIND_COLUMNS = {
    {3, SQL_C_CHAR, table_name, BUFFER_SIZE},
    {4, SQL_C_CHAR, column_name, BUFFER_SIZE},
    {5, SQL_C_ULONG, &key_sq, BUFFER_SIZE},
    {6, SQL_C_CHAR, pk_name, BUFFER_SIZE}
  };
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(constraints_BIND_COLUMNS));

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
  ASSERT_NO_FATAL_FAILURE(odbcHandler.BindColumns(BIND_COLUMNS));

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    rcode = SQLFetch(odbcHandler.GetStatementHandle()); // retrieve row-by-row
    ASSERT_EQ(rcode, SQL_SUCCESS);
    ASSERT_EQ(pk_len, INT_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);
    if (VALID_INSERTED_VALUES[i] != "NULL") {
      std::string expected_hex = GetHexRepresentation(VALID_INSERTED_VALUES[i]);
      ASSERT_EQ(data_len, BINARY_BYTES_EXPECTED);
      ASSERT_TRUE(BinaryCompare(data, expected_hex, BINARY_BYTES_EXPECTED - 2));
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
  for (int i = 0; i < NUM_OF_INSERTS * 2; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i % NUM_OF_INSERTS] + ")";
    comma = ",";
  }

  rcode = SQLExecDirect(odbcHandler.GetStatementHandle(), (SQLCHAR *)InsertStatement(TABLE_NAME, insert_string).c_str(), SQL_NTS);
  ASSERT_EQ(rcode, SQL_ERROR);

  odbcHandler.ExecQuery(DropObjectStatement("TABLE", TABLE_NAME));
}

// Disabled, no conversion from exteremly large value into sys.binary
TEST_F(PSQL_DataTypes_Binary, DISABLED_Max_Table_Creation) {
  const int MAX_DATATYPE_SIZE = 8000;
  const vector<pair<string, string>> MAX_TABLE_COLUMNS = {
    {COL1_NAME, "INT PRIMARY KEY"},
    {COL2_NAME, DATATYPE_NAME + "(" + std::to_string(MAX_DATATYPE_SIZE) + ")"}
  };
  const int DATA_COLUMN = 2;
  const int MAX_BUFFER_SIZE = 8192;

  // Bytes expected = (2 * n) + 2
  // 1 byte takes 2 chracters (in hex) + prepend `0x`
  // NOTE - BBF does not prepend `0x` to return data, whilst PG does
  const int BINARY_BYTES_EXPECTED = (2 * MAX_DATATYPE_SIZE) + 2;

  // TODO - Expected needs to be fixed.
  const int LENGTH_EXPECTED = MAX_DATATYPE_SIZE;
  const int PRECISION_EXPECTED = 0;
  const int SCALE_EXPECTED = 0;
  const string NAME_EXPECTED = "unknown";

  char name[MAX_BUFFER_SIZE];
  SQLLEN length;
  SQLLEN precision;
  SQLLEN scale;

  RETCODE rcode;
  OdbcHandler odbcHandler(Drivers::GetDriver(ServerType::PSQL));

  // Create a table with columns defined with the specific datatype being tested.
  odbcHandler.ConnectAndExecQuery(CreateTableStatement(TABLE_NAME, MAX_TABLE_COLUMNS));
  odbcHandler.CloseStmt();

  // Select * From Table to ensure that it exists
  odbcHandler.ExecQuery(SelectStatement(TABLE_NAME, {"*"}, vector<string>{COL1_NAME}));

  // Make sure column attributes are correct
  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
                          SQL_DESC_LENGTH, // Get the length of the column (size of char in columns)
                          NULL,
                          0,
                          NULL,
                          (SQLLEN *)&length);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(length, LENGTH_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
                          SQL_DESC_PRECISION, // Get the precision of the column
                          NULL,
                          0,
                          NULL,
                          (SQLLEN *)&precision);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(precision, PRECISION_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
                          SQL_DESC_SCALE, // Get the scale of the column
                          NULL,
                          0,
                          NULL,
                          (SQLLEN *)&scale);
  ASSERT_EQ(rcode, SQL_SUCCESS);
  ASSERT_EQ(scale, SCALE_EXPECTED);

  rcode = SQLColAttribute(odbcHandler.GetStatementHandle(),
                          DATA_COLUMN,
                          SQL_DESC_TYPE_NAME, // Get the type name of the column
                          name,
                          MAX_BUFFER_SIZE,
                          NULL,
                          NULL);
  ASSERT_EQ(string(name), NAME_EXPECTED);

  rcode = SQLFetch(odbcHandler.GetStatementHandle());
  ASSERT_EQ(rcode, SQL_NO_DATA);

  odbcHandler.CloseStmt();

  int pk;
  char data[MAX_BUFFER_SIZE];
  SQLLEN pk_len;
  SQLLEN data_len;
  SQLLEN affected_rows;

  const std::string MAX_VALUE = "decode('B1799A2D56B04D701F1328D55F07A09917E0F8B7590C1EC9947E360A639344635DC2897EB14DAAAB2E80DFA145BDAFDB3210D283407EE1104CD3682A0B85331F0E91AF4F53E26564CDD0332561B8D45A8BC23DFE3C66ADBE0ADF0D09F4C78B41BAD1FF0F4B368F34C81F85DDF9670AEE1867609BAC3DA0AB0844A7B2A38759A725B5CE945C83F9625A91178B4D696A95F34E5A5CAB6B06F9ECC07D97A9734AFA443F52FF6A3243AFDAA04E2DED40A694262B9B7FB8CEF7645DA8535A3701B1CD87D6646AB6374560C30942C507C6AC55F0FB882B599AC14838ED1420BA0ADFE5602EB10B00A38399E48F9352E1022EE66799192D2F221C56E563E60FA7A9B4D4A5356D55A0ABB7D9550ED500022C95B03422DD9CA6EA9F2F09C1CCA2A808E2D0623D0D17F68020D2A1F30D473DAD16D0C6C3FC6C7D34D7726498BB98B78E8B8CD06DE815559BECE82AED8C3281F54EFF6F91762830C5A24AF27AE5E381A848D6CE7215D03C95428A56B015B253E5EF4D8E2EE0A91F57EB7AE3DD603A64F15CDF5396E1947755B6378387B2EF805A1C6A7F7B43C878BA271C75C93649CF590D92DBB76465F3ECF874CEC38B2F7FC3513D537926CF41AD82C0C741676640BBB9E6243CCF7905EE6265C70B444121D9B17AA6D8435A7FE636D0E3ED2B926F4F6D4CECA27E55E496C750D066C4CACA48E8DC4BA82DCBA70CC5A11F3417D7945D95FE19CB8C4EF8DFF5BFFE52884791B91EF8AB515461B216DD778E6935A0E5CA48F3EECF20B780376BA4DDC27F834DE40CADBEA45CF5EF10DD3ED74B7083A433889635FB69B826C24843EC2C54650FC8F12E63EFCFF34B54ADA683136E0292E47A3BB7DC7633F60B1C71F0E49A566B6A21995DDA9A938378E74FC6FD2B3166BBCF79150E25BC59E8AF051BD82DCDE15A44AB570305B60D7CAF7FF1F13919D5EFE9900518BC259697710B767E06D74761D24411EE2D7B9AE0A8ED7B13729C2A4FCF98C43F8AA61E4F64AFB5B59C71CD8B989EEA74F87D91C415CBEBFED10E8B35AB23BD06BFEAE801FB4DA77B287D3CD1B4DFADF526682EF3D7513F9895D6107C1BF05662AAE1379280DE4361A310F1C4252A4FA059C984F2086E976C50CEDCECBB7629E04EAF9AB2BDA7426C015B45D7AE47E78BE449D6E7A01F5E54D5C9CE151D16DDC221129856FC0CCD2D116714C13F6B7A04640466CE36C4EC705988FD52243B3372BF7CF08A5309D279325A94BF95703E4893BA383A475703E06A6A6AEE2F64DD7F4BA4BFA7BA7B8F33CB2F655B3FBEF2BB98F67B60A2644C0548873C2AFCD94DAE0A03377AD944B1123A42F22D2B1D2316B790806C048BCD3A8DE291773D97E13C7148E62F502E8E3F6AC77EE89DD1DB84ED914308E5F330E5129388907B21A0C276324EACF16CADE2A7A708F05BD2D18A8C2833AC2F94A73BA6FBAAF8EB25854A36DCE785BADEDB90967E3DE7ECF44495A8C624822693F12A82671A0BBFDBA4D2FE0BB9A791440FA15C6498A766162A0384F0614375A5166F99942663717630D5E2D2816D38A8A14C0501EDDA4208F42C3164E853E53787509FDF09BBFAEDE8C72C4D62477D93C31562D892288C3574583C544A110BBB28D7585CFBB768C19844809362B55119AC11B3369ED25D8B156097392E26655659B0CF8C4002AD86D7AED003E47E0BBB0FD8F380A980D3C438BEB15C11390A9CEE22812CF6C11A0D9193B61DD3B973659403E361035F1F08AA666E476937C5CA1D3A568069DEC31FC723454AF1E30960DDB30AC3803010C142D2D9FD8C50B170531A49CA867CDC6D41138CF9108D1EB49EC31A1F565AFA6B8E538B3BA872F1681798925C2465C57CAE9F278C982538C9DFDA3E0C2A2B9EB4D80522F89D937390018455F9D7D35978D43E4311A8ECFF75EA43ADD548B93322900002EBE66240E7C17AB3B4F982689C167C1CBEEF4BEC8F2865E02B1AEBB6AF6D7CE488ABE870D4CAC71DA6012F29F5811CAC1AE32B47851C4854D4CA4C35F6B1D1B913D2D0EB3C95598BAC12FE64DABDEFF1F4AA937B7695C09BACE05C4E2F646BDB85E94AF49BA0598CBCA1C682725EDB889FCCCDDAB338A646F9844BC7BC4AF6EA62F36CDA5F62A96B18CB66E4CBA23DBF995867D38AE66F1F96597C8E41301276BFCCF7A8CBAD85534AFE7AFB7613DABD10B296C9EF42F273FB5D7D291CB47ED22EA18AFA0DA81093B2946EB25211FB41CB6C4722A0AAB886D1C875D6FF52286A8162B5A4145FB6E4E6878DFA7F9AB0D0B309D7DCE033FF9CC1347DB41CEA1357B7CC4867D151D91415ADD2F777E36AC5B5A54DF74AF370D56B71216BB8B6E23ACE9A876E8D401BD55A4A0C03CCF597D4AECCEF6F43988849580EDA37050017AAD5127CE65BC820DE8DBC6BC5F6718CC9E79D570DBE0CF48BA25184C2B9FE494B7CAF0FC4F1A22515442C32FDC183BD31C571607C205C19DC3722526235A1B1F519C8C750244D6DCEDF8F7723F07792C970DDEE53EEFE3D57AEBBEB41F858DC42EBC423CAEA2B77FBA492EF6298533A21226C75A94D5864F68D81895FE4473A4A9F7D62B1FA2E291A2D94CF7C38BC0D20C90F0604D213D7AD86C49DB40C125375DEC26300895FA9A4A8A4FF1A99AE9353B57EBBCFCD3682A654DA73C1B821981445397EE2D0ACFAB48AB0A08A9CE8D67F54C270ED84D5DD934B14BEDF236EF542E96200A01A21F0E200E52741FF14FF5012D506C14EDFE4AD3927E66F8B10F87FD218BEBF3051839B2547D5088B27A7F919B2DEDE08456EA3CE4AED8BDCE15623A687D6FA3B7D75C83B16321D62AED600C05BB92A10787E4B4BEFD59DAB42B97D0033DBB85F0E0F2E5CB52845BB8561B6BB5F91140100187ACF4777DBBA46EAA5DEBA7BFF0903A0F7B58617EAC7E00D0D1AF69E6D5FEB40252DA026C87AF4A2C05FB0C72E5AD160F53B7F4DC3F30EDA434A985132625A3861AEA910872C7BFF82A0DD1F85CF2782E2A986C7D3B7C75DE22BF00A6C418C2A6B00DF888EEE6D45F1C5D7AF185DA2332456FEC922063207CE04C684BE8288C3BF92AA5E01A524282368FD85EF337E6340E932B0CFE3672CBD0DF9609ECC621C2087DA148597D92175DDB27FAAC9F017565830349A27B894B119078299BCA0CCA968FF37A4C3B87C871A887C849F60B5702D7C3D9BD257420DA22A79CFC6CD19CF962CC3CAAC939F89F483FA7DAFEF78B3C80DD0A6C09A0EACD0375B5188C7276D5F2D3FEDC5200CD4FC1AEACED945CC05398251BD529FF4338429662BE64E916E968CA49B68DEE397FD79E500F3C1E17646F36718F1CE585EE860B81809CA85414D3E1C1BA1C18B5C68978A637D654AEDDB56F67F5B4910511B37D7C895DD030B24EF217FA1AE2DD6BE429D8F18F24841A7435CD07CF17CE909B610DE547BE920EAB18D5A7BE73D415042F7A34F25D76837C675C227DF36E07ED2360773E8EB0A2CDAD9F964AAD460422FED39D2E48E960E091279EC9FAD265A923835681DA383FEC37AF9031D8E84C7EF73E88F4D9250D0FF6E207CC74D13D5A9A7C10BD968753482F5434CC6F48BE4B18221665B9C64A35B9C51B8E87985290607F56CFDD4044F56A506356E02564C6946292E264B218E5333A4D5BD66721ED561C2647E8139DCA442C8C088C2A13929BDC42FB300EF07DC5F598D5CA1E89D3532B371F88833C8973AC757225D8A07133765CEE9C9B2DD9F14F3CC5994F5EF0D12ED21E55107D3E0C1E313C1D04E089182F303C35B63233E0CF022F3B831D480C60668A052B206186B2780BC4ECC3B2A7CFF401BADF3FA93B216EFC8684659E4BB5E1A2A893E2C3BFE0C19230FC14A5E36F2168FD6C102821371D2EE20D211F3C071289B84FA8E19AF7796089563D327E9117F4B1B2EE24A2A6837FB50F9E9CBC20F6B8071402770043E935BC1D59F0F6F2531C651D945D15DDCC250DB548B91D15B3CEAF618FC83BC7FDEE76EA7CF1F4D4A028ED0C641CE3B24A3EA1597B1EA6989247C42B3214BC9410AAF0BAE9941B7C0F50F6B90BE64F3AB2C5880F5EA968E02F83A6DE15CAE3B8EE68C3B8968FEE6C849408F0FF74CEE4408FF475136C70B4BE00B034BC822E9AA3DA094573167552ED5DCE0ECEFFD439F732ADED21B5C2E75BD3031400CB5A66F01E59129065891E81C466FB360961AE8660084ACB4997D9D1552BF2CBCC182E6C83B7578D6202F9043CF866B5E66E4E92F7127108A08BB6815E3C40CC7C9DFF90FA201FC912584CEE6411C1CEBE52DEB03A60552CBD6C2BBE535ED7A2ED5FE6423FF44C0BF08D52E8C078149F7CEBBC0B77E55D1DB3A0EE7ACB11A521FD3427BC86906573180ED9614B7D5A96291826C49A2BF6679ACD0EC2CFDFEAD49585A8E0D8909A86F09DBF6118B448366C2ACEA13EE68AF6F2571E8DC43DE82E7A5FAAF0947706F5D05D49111C7E4C65261FCDEBA49E7C83C6D672A73618C6AE79F583B3BE859EEF9114C0225B99AE21BB7E1FCCFA61A46D40DA6F7051C08661C195CC530CDB4DD0CAAF9C0BD7C021585FE60D2E89C6C409C30C47076C401E70DF1FEC68768D7AFCCF10A66759D52AFD87690EB6CFE63E90995D6427B877D47D7980DA99E5FB4BA943EA65C0C747A587767E51BADDCEB1C3BDC83E213AC7BF1697EA96CAAE58CACDE9041391C737CDD46AECF948E6FE378827BC86ADBFB235E3DC8AAE0C35D85916856369509A2B8D8C76A598668975C669A4701BA8353851DEFDE713209B0BDFC5CDBBC21EA6DEE5B74AA379CF8030058CB134A0870F1DCB5CD877897E1A281853C86327FDE74861E3E655A77B13EEC4A94E05FAAB399CFA2B3D03F5937CF81AC9F7FA7F3C6795C7DD0D4D047BEB0048F02CED257B75A516DD567A8071513193ED20CFA6EA8C2FAF37544EF2F45886C9202B4C5F01FF49C7A985638CF6335133B0FA816EBB4CCFED999FFAD72CBA5D42C8668E770FBEBB117A8A163A12FD79E3DD1BEC85265146F68A6EFA3E2BAB1C9B75B3F390A619AB2D6DF6AD98D5EBEB1DF8AD10E3337D3540FF5DE2639E7962AA564E84E8867E60B1B97A27326733353DAE3362C8A4855CFC03F15B11AFA2B103EBE3E2D1A8B6D285F7B9C39495D6620DCE215C46A1EA7DF23AF93866DEDA124A93CEA6DE01058994E939ABB46F97E3F02FC0A6BF52952DA03D79C25B36700CC6A59A9874E99F5E952A80DC842EC4D9EB715CE70A016D09406A5C9C6DF8282DD15DB5EB3BAB10855A7BE4764A096A3B935ED8C4236450416E1CF90AD061E21CA1091800E6B26EDA649953527D69AF7D0F5DA17FC04E60574EEC30A046CB0E1108198F20AD4213A2D0458B84B7FB7084FC785C0F6C7269308E9B35C2B51DE33980DFD6CC99242784FC4422BCF848397AA861AB45DBB983D24C1F10A60E3BEC9D7B70906AC3D539E3B3C66EB10FCC6C39DA46C6047AB369883CC0DE32974F6991877E93DA4EB0BE1ACA2E4D08C1CF38001D4C19403417FB9DDBA2C85C2DB20AB18C229BD087DD3C013986378BA3B284D0A839214428D820F6ED094896942864A0D159FAF02CA344D01E7864A79E92F10D2AB95DB7C2A042EDF3367DBDA6B63AF3B77D945539C10EEEEA13E3382475B19F0521B95241DF76B8ADB1F5FC2B4B9E50B4DF8ECA280410F6747EA726821101C5B0B5E430C6226FC3D3A7C08EE0832EA3ABBEB7072A0649C55ADC49C11A6C48FA56C88B854A9990320CFE78D1CA1AACC856E5A5BBDB9B3A9F983335B25AA44CDF1014D26A802CC9D2CF3D665AC7698A3706018867678013207A9B8B244BAA1197263491952D3ADAFAA7C09C85B2E0DB98B81C496EAB95BC50CA143AEEE2A7B85C20F85B69FD1C7D62A8BDB29DD247019A6E617C436430767B39B32A6CFDD100EB77AD958D4350541D47318D96B743016CEB188D2F8BEC4704A943772F387380B9F17A8261509AD339BC5A9FF1D2C12591C3AC5377FC8742695895AEEA4A7902B0ECDE10F111581027F5D37DFCC892231623EC1CD25E87495C52491A88422C2FE8A03CE9B86EBAA0D9C407369106711E00E1E7EA5FDF7D7D3E3C25E5CEC30B4EEC808D38750C6C1046CF75690CBD9FCC12A9F1D010783C8D3B6DAB5840F13875F1A74A78150F98B82D5DA5F3F7A6C0114C1C94ABC7CBFB6F21A4FD76555121CD29226B419D4069CE8A1239967CA6DB944CFF3AD752E00A65E2BF8B112584D2692DBC5B62D3EDAEB19D850C7086DAD616176F79FEF7921AE3CCFE28C45A288BEAEA5BC38BC8920BDDAEE039E1E9BF707944863589E37A18F3CDA6EBB6C45D5BDFBD48CBF076D6A5906BDE9E6BDFE25A61295AD34E617C259786FB64E7FB3E60904C0ADC869A9E5B7FE60E53D5D1E8B9C479FF772280430EAC2415B8BC4F7A178AD0DF434C3BED21C945366B2ED8F69C12257B33E3827A635E1C406BC57780DA9EB7878F42F28073D5A55D0DDBCAE8F66D00B8847B82D16CD75609CB48BFACF260CB77B4E981F456A07FA83C8A406E1035F7AA5957F98C46156A514A259BCC252CD1C474BC512F62A939C2E40D17FF94CBD4CE90DDE3666335836BD29E0BAF71AF4DEA5D60DEBCBB9A0895220F89003837A7178A498F13008A1DE4252C1BB49E3541EF2051582861DB37639C145C58794133C7408397AD19FA6AFD2D573DEAF49DC72288B7DC751A9387CD152CF6D255BB9673646EE412FD910B487C36B30284D492A5FD8E1086FBC8D7BEBEFA5DAB2653A72D042A9FEFE34E880090F2A505581A3220AF9E6DC7ACECC13206489E77111233BF2B7AE67D77EB4F02F4F835D8932A9A5F337AD2A0BEE942FEE5220E50D25F814FE90AAB7FEBD29ED5B7356ED79D3BB1123684C9D73100482FDDFA32E707D76883A2F60E5E3821A7A40BB52998676CFD9EB9D5EF25A12206E4D39E4ACDB8108588797FEF91FC8A2B391AD5F9A13F296D14111170A8FDACDA6C8139CA72B24F3F34FB3DD58C3F1F903C69CB66194535F9F0913BE4FB833C32B9BD1506957F2B7F0B1100B85FFA243C009A965704C80FA8D4793F0A83762A9069536B08B3F54FB3FD1B6C094052E5CB4EEF08CC57414514C075CD465002A5D3045A51FA3E523B7F8CBF0883A69710C1C8C30DB72A4E6C6AB3950CAA925FAA99D7B461A5A384EAD77B6CC54E431D61A32A443B1D41DDB4FD28F49C7DF6B2211E2850F3BFC48F529513A4EA9ED0610899DD4E89B8D96923A2B84CE4DC960720D256C7CDA8F4E7D87548C4A90AD7CCF34AEC2136AE6A59C19B0E9FDCFA2BF69B5B0C5D5E98BFD95AC5BF21FCC8309D1900DCAD9AFE083E44CB4219CB1413DF94DCB8D2E2112A487D00F7827D311360C1E768E5B76E94D0BA97C30CB7168FEDAABDD7E683B9FAE65B425D8B29D64DA8997B5AC8FEDE4F80560DF82EF82E16A5268E7C0EE4F108353978FADF8E802FF79A837AB56CCACEDF2751217AB715D3BA1055E0894276E337FED2680C83D1BF4004B9F639F6F74A933C4342A6CECC83246E7C020311B5CDA3E1DC352196F649B06A0169B05B140D69FCDE7D19AD06FBCAA0B9721C9FD7EECF87C7DF61E8D8C962EC83D5F0200EE45E701513E29DAE76531CDFE4E7B65392EC3CA59BD62AA7DF0B12C8603A50EB5D27BB5579DEE0F7B3DEBF63F55E2D8A51B2F42E4CB40A3FDD70D6E96D81346D805BE3735D599EEABD9C9BCB129DA6686C5E6CF9589DD44D91D398EC97612299584B5AEF39C92D4B5879D37F9DFA71E418B658FD5ABE57DF9BA0A256D58F5240DD712819F3D9D17350A4E018E6908E3412B3C1DEC611799C0EB3353B4EDDA8F9ADF43AAA09F691BC40A5B06388F01A4BE7B5B369F27C632459D73AFB1761DAC7CC8EAD94C8C65682A5F382FEE5DBACC7AFAF9115402DFFA391B88EA6D0C56578E0EE879EA460C3DB17627D9705C493A9890240A246918128E7CAA6DCE7D17C53787B0FECC6269CEB34D8D90827A229210E9A725A7C405BFD533439A003C9C44125C3CCABDFE9F6F8CBA6D77545B7F56A8299806BC63AD661703FD718A39F82B82373C5152333C25723712C277C5127B19083730E61E6EB92EC8FA131ADA04E6BB900ECB7074F4C9F501C391EE0789B7F6D82647CA8CD31CE23C261C3C05BA17CE1F4F379DE78FC567887B841A46CC741BBD957DAE47834E3D61451BB91B03E0ADBD7AFA705BFAEFD768254119DB28E84F9C9B4EC74A17D0683B15EC12B4AA92C469097E3969398BE3D9EFD38E3EB6DF46F8EF3B1EEF84E8F444FAEC31C2D6FF035D649F3330DBF8BB1DB8F1110E624CAA90A3DD07290A34DB9CF81DA426AC2D03ED3EFD65D21E587A8A2200E3480EF5F5A48269E9361DD1D3AF90F815304B577F5187FCF4CB79B501ACADA2EB27040E197BE51A5A5AA1B2BD10B1074A43F839B057D333735519D1991963710EB46286527C5C660A4ED9C40A478C51558285C093C4C23C6A9C58BB00BC71F2BEE64AF7871ED8AB4D524B6157C83646C49AC7509091500E303F6F703EDA7AE418D6D296580266BE49257B976429A4765B6119CB7DBD83C43C97691BC3AE0FB411CB92439D06A242713E4B20DB3B0865E2AC9A0C4AD8BA91CC95867BD6757AB987C0E9BF12E03D903EA66EC0DB3516354EF971A2866CBAFF81AB1F80AC8017110973338529BC6568ABE94E2AC536912B41FF85BC099A07E8C553441977C008A48FF66E7DBC6F58FB17ED4A26928E239E4B507E3144284BA98562F2D4D645EDFCA876C550F68BF189EF7C5B7BCA9F63008BDA62C1FA1D3CF742B8E05DC8C24F9149EE3ED7EAE356564EBDD76F4B4BB1F8EEB42607E98AD54BC3478669F81CDFCCB81AF5AD22E4E73693C0FE7BCBD37B86F9B775FB56A67504E52289089FB643E28A620721CF13F5864DB3221DD70218D37C52F72FD7D20033B53EA3CC8027F9E3CF12074A76C18F6928D62439C6C7D3CA468D8B887556D9FA2294DABD1E8124427C754B9D6AD864C5C7E71805D0148CEF823B5BCB0F7222965B645810B79FB6DCF3F69FDC858190275697CF3600CF793060187FE2BE1A0C7E7EC13005AB5674454FC70B385EAB4B5F4794A8E8FDBE011CADA7A9F376CD4F20A709A4CCBA74437E5F6EB881F0166C7303571D80A8292B728D57B38F0D608962117872743116A4B32BE70E1FA1B949F139783ECA2A7329DD101E8D79C183550AA9FD5F7E14BEF9D3BAC2E7DE5581C182C16C421A3DBF7BC12A80A881A63ED0CB6480407654E7A5874701D28C9C7CA53D7DE67129C9A0384854E858E0DDF0662DD4BD964AC1EB7E4FA896008484E3C960288BAFE316A9B5F07BA6FFF6935C65C8C15FEA37ABD139FA7AFFE05F3CB0E75D4AA5432DDBAAB205DB1DA35EB24A7F33648C533E5B750C1B4AA2C3930D4C19E05A30A16351CA3B561AB59FF8BD79C310F0961FCEDE89A9CC62DA6AE4C186B501B2D5C4F4A6114DEE4E8FDE0A1F1C99B9EA5D47DB7090B2209CACF2745324694C3BA0F5EDF0EB0BAAE74D0295B25833A569DF79AC546326F99D2FD9C086C342EA67E35817BE051AB8405573332C669BB20CD611A7AD86BD313EF7CD3239A085EB976D671A5489E0485FE2E2455534C4F852D21F92CDC8C0657694884A299D8909DCF84BFEF7970FEFBC406C6E469A551DE59AD6A497DBB7356396CF2504DA6239F6B069EC6ED89AF31A3E032526799612959A29F8D1BA423ADEC5752978BEBDCD8285370D53E4F29CD634E677137B3368C4B4DB3B3130ED18915C5C64BD153C7B9DB52668E17D03C4531774EFBE4E1790E9102CA8B4EFAD4C81E7E95BCE3F0FDB8DE8709F267D1C5BC257907EA553A70A6BD3EB6DC20031E4341C010A8028060DCDB6F7879DD156F40441BB693BD01B05CB6793742CF68C5B173F2F5A1C04295DF1B1AF2471D6875257201CAD45C60337CC02C9A41024B4598F7FF3AF4DD4ABCACE46188F00637C5E8774520EAC2E3922A7164409F329703B4E39F854AAF75B699FFF8155DD383D8E6C1115116AE68CAAD864E8A0B67879D88D5AA33FE529AFCD0C815E28D691C1C9032F1F0526E611E50291C91A4282EF8B171D609E6160A00DD10A0B40CE73D25F7AFFF6E4B983EA75E065C2B6E7A6E35970E2441F94C4E7D6D64749BA91E650E8323449184E7E6444C9ACCA12F15AC2F2856F79E8C67A9948FFB7AA85EE8A1FBB485DC3CBBFB51CF794D925097A3E08B018B040176FBDB412D6CEA51CE5B8F58640CDD7A9EF07A19EBA2609F8144BFC7AEEC4940E2C9D2FE14C156526F22F99DDD1F74CF5B557B478855FFCA8D1C00E674FCC6A90278C7825F3A4B7FC35AE5753AAD3515B1EB29B5FDA06CC16A4E91D1154C3412F3A8DF0F68FD77D8D36317BD3896746CFACDBB5938305250525536AE3558C90863FF7FB8FBFB348E9B579D8439CD6C698D44450FF5ADF429B0AC3BEBF2A170270D53007B2F1EDD93C724416E4680A7FF2FB393906CB72D8E587A6E58ECD7D3541C8C3566118D4E35436A16DB285C2C7521193B1212E0D3E51A4C9C0A539DA32C440654C15493D9B8F3D19FFF99C879D46E1FBF0AAC52569156F21E16D64E66D8C61D5505C419F6D3021457B0FA95AFD3069CCE3F2A368DCD4825DB58F9CDCA9073BB480FCBFEF2E8ADEBF9C809752667F62E12A6462483F6AD7B1DFE6066FDE98AB32B01185F63024220E10ACA915DB7286A91AB8ECCD67815AD2B346897475C8DB2DF5BB3C7CA928D008FE87FEC966F85E3FE034BCBA95FC44010C15B13085FF83FA1B219652E72EC0FE9FA01BAB64ED5DDB283442099C0C7DCCCABE9F15BE47C49B7BB030EDB42AF37AA3ACE5AFA51CEE9F72315070ACAC4868ECBB491AFC7753AC15A9A8C9FF627D9C22DF428D36E3201B5451B1737B73D4992A12E32259E3A5CEB157F861169377110F6C81A66F4BB1C57E64855F5BA13A10041C2540FF684D88F85D7B2B078F2CE1F2A6DDDCC1420A53E7BB0DEF4C1F6E4895FB8259CD7F530A37CD3955FC854CA6F7812B372AA66085514E17191C038C386B81D3B48A79082727E8FAB2CA82B9E4BBBB14CE79DF93AEE2435D0494C42036C9ACD82C908F5F6DD0766C7520C0BB0F47669A36F8217A59E117FE3C27E40030B2C5F0EF5832A81C2D70A4033B000860236258584961B3B409020FFF4EE49E8F5F8F5105DB98738D79AFD15A5D03DF5C34174328133D5797B60FA1B5E8F920F9C6AD1EF2BD6B8255E1A5CD2745312FFCED53DF3A78B3181A5C8958B35061C4F1F20A571B3748417FF65268D3B5DF9A910213E648F58EBBA41F8667217A07C67B658E938913DA9CFBC4E803B74A8D048516515E61F4EBB22E089CA0630FA65AF4E48AF421E3083C1613A24CF4FBE88F460A03C1D757305C53E39D415A33EB1E80F6876C41BF872DE6EF', 'hex')";
  const vector<string> VALID_INSERTED_VALUES = {
    "NULL",
    "00",     // Min
    "0",      // Min, different format
    "46",     // Rand
    "02",     // Rand, different format
    "255",
    MAX_VALUE
  };
  const int NUM_OF_INSERTS = VALID_INSERTED_VALUES.size();

  const vector<tuple<int, int, SQLPOINTER, int, SQLLEN *>> BIND_COLUMNS = {
    {1, SQL_C_LONG, &pk, 0, &pk_len},
    {2, SQL_C_CHAR, &data, BUFFER_SIZE, &data_len}
  };

  string insert_string{};
  string comma{};

  for (int i = 0; i < NUM_OF_INSERTS; i++) {
    insert_string += comma + "(" + std::to_string(i) + "," + VALID_INSERTED_VALUES[i] + ")";
    comma = ",";
  }

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
    ASSERT_EQ(pk_len, INT_BYTES_EXPECTED);
    ASSERT_EQ(pk, i);
    if (VALID_INSERTED_VALUES[i] != "NULL") {
      std::string expected_hex = GetHexRepresentation(VALID_INSERTED_VALUES[i]);
      ASSERT_EQ(data_len, BINARY_BYTES_EXPECTED);
      ASSERT_TRUE(BinaryCompare(data, expected_hex, BINARY_BYTES_EXPECTED - 2));
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
