-- This script will clean the temporary tables
DECLARE @sql VARCHAR(MAX) = ''
DECLARE @time_now DATETIME = GETDATE()
DECLARE @id_suffix_length int = 32
DECLARE @expiration_minutes int = 30


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
	  ,CAST((@time_now - create_date) as float) * 24 * 60 as elapsed_minutes
	FROM sys.objects
	WHERE
		type_desc in ('USER_TABLE', 'VIEW')
		AND CHARINDEX('__', name) > 0
		AND ((LEN(name) - CHARINDEX('__', name) = @id_suffix_length + 1)
			OR (LEN(name) - CHARINDEX('__', name) > @id_suffix_length + 1)
			    AND (SUBSTRING(name, CHARINDEX('__', name) + @id_suffix_length + 2, 1) in ('_', '.')))
) as TEMP_TABLES
WHERE
	 elapsed_minutes >= @expiration_minutes

EXEC (@sql)
