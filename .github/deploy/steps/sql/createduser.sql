IF NOT EXISTS (SELECT principal_id FROM sys.database_principals WHERE name = '$(Username)')
BEGIN
	CREATE USER [$(Username)] WITH PASSWORD = '$(Password)'
	PRINT 'Created $(Username)'
END
ELSE
BEGIN
	ALTER USER [$(Username)] WITH PASSWORD = '$(Password)'
	PRINT 'Updated $(Username)'
END

GO

DECLARE @ReadRights AS VARCHAR(MAX)='$(ReadRights)'

IF (@ReadRights = 'True')
BEGIN
	ALTER ROLE db_datareader ADD MEMBER [$(Username)]
	PRINT 'Add read rights to $(Username)'
END
ELSE
BEGIN
	ALTER ROLE db_datareader ADD MEMBER [$(Username)]
	PRINT 'Remove read rights from $(Username)'
END

GO

DECLARE @WriteRights AS VARCHAR(MAX)='$(WriteRights)'

IF (@WriteRights = 'True')
BEGIN
	ALTER ROLE db_datawriter ADD MEMBER [$(Username)]
	PRINT 'Add write rights to $(Username)'
END
ELSE
BEGIN
	ALTER ROLE db_datawriter ADD MEMBER [$(Username)]
	PRINT 'Remove write rights from $(Username)'
END

GO

DECLARE @CreateRights AS VARCHAR(MAX)='$(CreateRights)'

IF (@CreateRights = 'True')
BEGIN
	GRANT CREATE TABLE TO [$(Username)]
	GRANT ALTER ON SCHEMA::dbo TO [$(Username)]
	PRINT 'Add create rights to $(Username)'
END
ELSE
BEGIN
	REVOKE CREATE TABLE FROM [$(Username)]
	REVOKE ALTER ON SCHEMA::dbo TO [$(Username)]
	PRINT 'Remove create rights from $(Username)'
END

GO

DECLARE @ExecRights AS VARCHAR(MAX)='$(ExecRights)'

IF (@ExecRights = 'True')
BEGIN
	GRANT EXEC TO [$(Username)]
	PRINT 'Add exec rights to $(Username)'
END
ELSE
BEGIN
	REVOKE EXEC FROM [$(Username)]
	PRINT 'Remove exec rights from $(Username)'
END

GO


DECLARE @CreateViewRights AS VARCHAR(MAX)='$(CreateViewRights)'

IF (@CreateViewRights = 'True')
BEGIN
	GRANT CREATE VIEW TO [$(Username)]
	PRINT 'Add CREATE VIEW rights to $(Username)'
END
ELSE
BEGIN
	REVOKE CREATE VIEW FROM [$(Username)]
	PRINT 'Remove CREATE VIEW rights from $(Username)'
END

GO
