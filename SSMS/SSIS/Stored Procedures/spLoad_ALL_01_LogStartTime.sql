

-- ===========================================================================
-- Author:				Ivan Erik Kragh
-- Create date:			Oct 19 2019  2:41PM
-- Stored procedure:	[SSIS].[spLoad_ALL_01_LogStartTime]
-- Description:			
--              
-- ===========================================================================
	
CREATE PROCEDURE [SSIS].[spLoad_ALL_01_LogStartTime]
	@PackageName nvarchar(255) 
AS BEGIN
	DECLARE @message nvarchar(max);

	SET @message = 'Started load of EDW Layer...';
	EXECUTE dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 1, @message = @message;
END