USE [master]
GO

EXEC dbo.sp_msforeachdb N'
	IF (SELECT DB_ID(N''?'')) NOT IN (1, 2, 4)
	BEGIN
		ALTER DATABASE [?] SET QUERY_STORE = ON
		ALTER DATABASE [?] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30), MAX_STORAGE_SIZE_MB = 1024)
	END
'
GO