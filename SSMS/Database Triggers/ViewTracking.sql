



CREATE TRIGGER [ViewTracking]
ON DATABASE
FOR DDL_VIEW_EVENTS AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @data XML;
	DECLARE @schema nvarchar(1024);
	DECLARE @object nvarchar(1024);
	DECLARE @event nvarchar(1024);
	DECLARE @user nvarchar(256);
	DECLARE @msg nvarchar(1024);
	
	-- Retrieve event information
	SET @data = EVENTDATA();
	SET @user = @data.value('(/EVENT_INSTANCE/LoginName)[1]', 'nvarchar(1024)'); 
	SET @schema = @data.value('(/EVENT_INSTANCE/SchemaName)[1]', 'nvarchar(1024)');
	SET @object = @data.value('(/EVENT_INSTANCE/ObjectName)[1]', 'nvarchar(1024)');
	SET @event = @data.value('(/EVENT_INSTANCE/EventType)[1]', 'nvarchar(1024)');
	
	IF ('managed' = (SELECT CONVERT(NVARCHAR(255), [value]) FROM sys.extended_properties WHERE class = 0 AND [name] = 'Customization'))
	BEGIN
		IF (@schema NOT IN ('dbo', 'meta', 'SANDBOX', 'TEMP','DM', 'Transform'))
		BEGIN
			PRINT 'Data Warehouse: Changes to VIEW has been denied: ' + @event;
			ROLLBACK;

			SET @msg = 'Trigger has denied changes to ' + QUOTENAME(CONVERT(nvarchar(1024), @schema)) + '.' + QUOTENAME(CONVERT(nvarchar(1024), @object)) + ' initiated by ' + @user
			EXEC spLog @subsystem = 'DW', @source = @event, @type = 'Warning', @severity = 1, @message = @msg

			RETURN;
		END;
	END;
END;
GO



GO