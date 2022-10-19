
DROP TYPE IF EXISTS table_type_test1;
DROP TYPE IF EXISTS table_type_test2;
GO

SELECT COUNT (typrelid) FROM sys.table_types_internal 
WHERE typrelid IN (SELECT object_id FROM sys.all_objects WHERE name LIKE 'TT_table_type_test1%' );
GO

CREATE TYPE table_type_test1 AS TABLE (Id INT, Name VARCHAR(100));
GO

SELECT COUNT (typrelid) FROM sys.table_types_internal 
WHERE typrelid IN (SELECT object_id FROM sys.all_objects WHERE name LIKE 'TT_table_type_test1%' );
GO

CREATE TYPE table_type_test2 AS TABLE (Id INT, Name VARCHAR(100), floatNum float, someDate date);
GO

SELECT COUNT (typrelid) FROM sys.table_types_internal 
WHERE typrelid IN (SELECT object_id FROM sys.all_objects WHERE name LIKE 'TT_table_type_test2%');
GO

SELECT COUNT (typrelid) FROM sys.table_types_internal 
WHERE typrelid IN 
(SELECT object_id FROM sys.all_objects WHERE name LIKE 'TT_table_type_test1%' or name LIKE 'TT_table_type_test2%')
GO

DROP TYPE IF EXISTS table_type_test1;
DROP TYPE IF EXISTS table_type_test2;
GO 
