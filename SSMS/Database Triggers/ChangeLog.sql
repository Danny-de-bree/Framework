
CREATE TRIGGER [ChangeLog] 
ON DATABASE 
FOR DDL_DATABASE_LEVEL_EVENTS --create_procedure, alter_procedure, drop_procedure, create_table, alter_table, drop_table, create_function, alter_function, drop_function, create_view, alter_view, drop_view 
AS 
BEGIN
	SET nocount ON  ; 
	DECLARE @data xml   
	SET @data = eventdata()  

	IF (@data.value('(/EVENT_INSTANCE/SchemaName)[1]', 'nvarchar(1024)') NOT IN ('sys')) AND (@data.value('(/EVENT_INSTANCE/ObjectType)[1]', 'varchar(25)') NOT IN ('STATISTICS'))
	BEGIN
		INSERT INTO dbo.ChangeLog   
					(   
								databasename,   
								eventtype,
								SchemaName,
								objectname,   
								objecttype,   
								sqlcommand,   
								loginname,   
								eventdate   
					)   
					VALUES   
					(   
								@data.value('(/EVENT_INSTANCE/DatabaseName)[1]', 'varchar(256)'),   
								@data.value('(/EVENT_INSTANCE/EventType)[1]', 'varchar(50)'),
								@data.value('(/EVENT_INSTANCE/SchemaName)[1]', 'nvarchar(1024)'),
								@data.value('(/EVENT_INSTANCE/ObjectName)[1]', 'varchar(256)'),   
								@data.value('(/EVENT_INSTANCE/ObjectType)[1]', 'varchar(25)'),   
								@data.value('(/EVENT_INSTANCE/TSQLCommand)[1]', 'varchar(max)'),   
								@data.value('(/EVENT_INSTANCE/LoginName)[1]', 'varchar(256)'),   
								getdate()   
					)
	END
END