#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <vector>

using std::string;
using std::vector;

// Trims whitespace from a string from left and right side.
string trim(const string &s) {
  const string WHITESPACE = " \n\r\t\f\v";

  // trim whitespace from left
  size_t start = s.find_first_not_of(WHITESPACE);
  string left_trimmed = ((start == string::npos) ? "" : s.substr(start));

  // trim whitespace from right
  size_t end = left_trimmed.find_last_not_of(WHITESPACE);
  return (end == string::npos) ? "" : left_trimmed.substr(0, end + 1);
}

// Parses the odbc_schedule file line by line.
void ParseSchedule(vector<string> &tests_to_run, vector<string> &tests_to_skip, bool &contains_all) {
  const string IGNORE_FLAG = "ignore#!#";
  const int IGNORE_FLAG_LENGTH = IGNORE_FLAG.length();

  string line{};
  std::ifstream schedule_file;
  schedule_file.open("odbc_schedule");

  if (!schedule_file.is_open()) {
      // ERROR: Cannot open schedule file
      return;
  }

  while (std::getline(schedule_file, line)) {
    line = trim(line);

    if (line.rfind("#", 0) == 0 or line.empty()) {
      continue;
    }
    
    if (line == "all") {
      contains_all = true;
      return;
    }

    if (line.rfind(IGNORE_FLAG, 0) == 0) {
      string value = line.substr(IGNORE_FLAG_LENGTH);
      tests_to_skip.push_back(value);
    }
    else {
      tests_to_run.push_back(line);
    }
  }
}

int main(int argc, char **argv) {
  string filter_string = "*";
  vector<string> tests_to_run;
  vector<string> tests_to_skip;
  bool contains_all = false;
  
  ParseSchedule(tests_to_run, tests_to_skip, contains_all);

  // If odbc_schedule doesn't contain 'all', build the string GoogleTest will use to run or skip tests.
  if (!contains_all) {
    filter_string = "";

    for (auto it = tests_to_run.begin(); it != tests_to_run.end(); ++it) {
      filter_string.append(*it);
      filter_string.append(":");
    }

    for (auto it = tests_to_skip.begin(); it != tests_to_skip.end(); ++it) {
      filter_string.append("-");
      filter_string.append(*it);
      filter_string.append(":");
    }
  }
  
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = filter_string;
  return RUN_ALL_TESTS();
}
