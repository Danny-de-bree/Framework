

-- ===========================================================================
-- Author:				Ivan Erik Kragh
-- Create date:			Oct 19 2019  2:41PM
-- Stored procedure:	[SSIS].[spLoad_ALL_04_LogStopTime]
-- Description:			
--              
-- ===========================================================================
	
CREATE PROCEDURE [SSIS].[spLoad_ALL_05_LogStopTime]
	@PackageName nvarchar(255) 
AS BEGIN
	DECLARE @message nvarchar(max);
	DECLARE @tableLayer nvarchar(255);
	DECLARE @tableEntity nvarchar(255);
	DECLARE @tableIndex	nvarchar(255);
	DECLARE @stmt nvarchar(max);

	SET @message = 'Rebuilding indexes where fragmentation is > 10 %';
	EXECUTE dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 1, @message = @message;

	DECLARE cur CURSOR LOCAL FOR 
		SELECT 
			[s].[name]
		,	[o].[name]
		,	[i].[name]
		,	'ALTER INDEX [' + i.name + '] ON [' + s.name + '].[' + o.name + '] REBUILD WITH (ONLINE = OFF);'  
		FROM sys.objects AS o
		JOIN sys.schemas AS s ON o.schema_id = s.schema_id
		CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(),o.object_id,NULL,NULL,NULL) AS a
		JOIN sys.indexes AS i ON a.object_id = i.object_id AND a.index_id = i.index_id
		WHERE s.name IN ('EDW') AND i.name IS NOT NULL AND [avg_fragmentation_in_percent] > 10 AND i.type IN (1,2)
		GROUP BY [s].[name], [o].[name], [i].[name]
		ORDER BY o.name
	OPEN cur 
	FETCH NEXT FROM cur INTO @tableLayer, @tableEntity, @tableIndex, @stmt
	WHILE @@FETCH_STATUS = 0
	BEGIN
		/* Rebuilding indexes where fragmentation is > 10 % */
		EXEC sp_executesql @stmt 
		FETCH NEXT FROM cur INTO @tableLayer, @tableEntity, @tableIndex, @stmt
	END
	CLOSE cur
	DEALLOCATE cur


	SET @message = 'Ended load of EDW Layer...';
	EXECUTE dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 1, @message = @message;
END