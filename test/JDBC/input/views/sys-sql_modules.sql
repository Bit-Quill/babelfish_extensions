-- Setup
CREATE DATABASE db1
GO

CREATE VIEW my_master_view AS -- This view should not be seen as we will be using a different database for the test
SELECT 1
GO

USE db1
GO

CREATE VIEW sql_modules_view AS
SELECT 1
GO

CREATE FUNCTION sql_modules_function() 
RETURNS INT
AS 
BEGIN
    RETURN 1;
END
GO

CREATE PROC sql_modules_proc AS
SELECT 1
GO

CREATE TABLE sql_modules_table1(a int)
GO

CREATE TABLE sql_modules_table2(a int)
GO

CREATE TRIGGER sql_mod_trig ON sql_modules_table2 INSTEAD OF INSERT
AS
BEGIN
SELECT * FROM sql_modules_table1;
END
GO

-- Test for function
SELECT
    definition,
    uses_ansi_nulls,
    uses_quoted_identifier,
    is_schema_bound,
    uses_database_collation,
    is_recompiled,
    null_on_null_input,
    execute_as_principal_id,
    uses_native_compilation
FROM sys.sql_modules
WHERE object_id = OBJECT_ID('sql_modules_function')
GO

-- Test for views
SELECT
    definition,
    uses_ansi_nulls,
    uses_quoted_identifier,
    is_schema_bound,
    uses_database_collation,
    is_recompiled,
    null_on_null_input,
    execute_as_principal_id,
    uses_native_compilation
FROM sys.sql_modules
WHERE object_id = OBJECT_ID('sql_modules_view')
GO

-- Test for triggers
SELECT
    definition,
    uses_ansi_nulls,
    uses_quoted_identifier,
    is_schema_bound,
    uses_database_collation,
    is_recompiled,
    null_on_null_input,
    execute_as_principal_id,
    uses_native_compilation
FROM sys.sql_modules
WHERE object_id = OBJECT_ID('sql_mod_trig')
GO

-- Test for proc
SELECT
    definition,
    uses_ansi_nulls,
    uses_quoted_identifier,
    is_schema_bound,
    uses_database_collation,
    is_recompiled,
    null_on_null_input,
    execute_as_principal_id,
    uses_native_compilation
FROM sys.sql_modules
WHERE object_id = OBJECT_ID('sql_modules_proc')
GO

-- Test that sys.sql_modules is database-scoped
SELECT
    definition,
    uses_ansi_nulls,
    uses_quoted_identifier,
    is_schema_bound,
    uses_database_collation,
    is_recompiled,
    null_on_null_input,
    execute_as_principal_id,
    uses_native_compilation
FROM sys.sql_modules
WHERE object_id = OBJECT_ID('my_master_view')
GO

-- Cleanup
DROP PROC sql_modules_proc
GO

DROP TRIGGER sql_mod_trig
GO

DROP TABLE sql_modules_table1
GO

DROP TABLE sql_modules_table2
GO

DROP VIEW sql_modules_view
GO

DROP FUNCTION sql_modules_function
GO

USE master
GO

DROP VIEW my_master_view
GO

DROP DATABASE db1
GO