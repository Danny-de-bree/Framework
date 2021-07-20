

-- ===========================================================================
-- Author:				Ivan Erik Kragh
-- Create date:			Oct 19 2019  2:41PM
-- Stored procedure:	[SSIS].[spExtract_ALL_01_LogStartTime]
-- Description:			
--              
-- ===========================================================================
	
CREATE PROCEDURE [SSIS].[spExtract_ALL_01_LogStartTime]

	@PackageName nvarchar(255)

AS BEGIN
SET NOCOUNT ON ;
DECLARE @message nvarchar(max);
SET @message = 'Start Azure Data Factory Pipeline: ' + @PackageName ;

EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Info', @severity = 1, @message = @message;

END;