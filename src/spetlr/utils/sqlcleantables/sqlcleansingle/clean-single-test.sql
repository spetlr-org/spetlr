-- This script will clean the temporary tables
DECLARE @sql VARCHAR(MAX) = ''
DEClARE @id VARCHAR(MAX) = ' {Config_UUID_SPETLR} '

SELECT @sql = @sql
    + (CASE WHEN type_desc = 'VIEW' THEN 'DROP VIEW' ELSE 'DROP TABLE' END)
    + ' [' + schema_name + '].[' + name + '] ' + CHAR(13) + CHAR(10)
FROM
(
	SELECT name
	  ,SCHEMA_NAME(schema_id) AS schema_name
	  ,type_desc
	  ,create_date
	  ,modify_date
	FROM sys.objects
	WHERE
		type_desc in ('USER_TABLE', 'VIEW')
		AND CHARINDEX(TRIM(@id), name) > 0
) as TEMP_TABLES

EXEC (@sql)
