CREATE TABLE [meta].[BusinessObject] (
    [BusinessObjectID]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [BusinessObjectSchema] NVARCHAR (255)  NOT NULL,
    [BusinessObjectName]   NVARCHAR (255)  NOT NULL,
    [LoadPattern]          NVARCHAR (50)   CONSTRAINT [DF_BusinessObject_LoadPattern] DEFAULT (N'FULL') NULL,
    [IncrementalField]     NVARCHAR (255)  CONSTRAINT [DF_BusinessObject_IncrementalField] DEFAULT (N'') NULL,
    [IncrementalOffSet]    INT             CONSTRAINT [DF_BusinessObject_IncrementalOffSet] DEFAULT ((0)) NULL,
    [LookupKey]            NVARCHAR (255)  CONSTRAINT [DF_BusinessObject_LookupKey] DEFAULT (N'') NULL,
    [RolePlayingEntity]    NVARCHAR (4000) CONSTRAINT [DF_BusinessObject_RolePlayingEntity] DEFAULT (N'') NULL,
    [Schedule]             NVARCHAR (255)  CONSTRAINT [DF_BusinessObject_Schedule] DEFAULT (N'') NULL,
    [PreserveSCD2History]  TINYINT         CONSTRAINT [DF_BusinessObject_PreserveSCD2History] DEFAULT ((0)) NULL,
    [IsEnabled]            TINYINT         CONSTRAINT [DF_BusinessObject_IsEnabled] DEFAULT ((1)) NULL,
    CONSTRAINT [PK_BusinessObject] PRIMARY KEY NONCLUSTERED ([BusinessObjectID] ASC) WITH (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON),
    CONSTRAINT [CC_BusinessObject_BusinessObjectSchema] CHECK ([meta].[ufnCheckSchema]([BusinessObjectSchema])=(1)),
    CONSTRAINT [UC_BusinessObject] UNIQUE CLUSTERED ([BusinessObjectSchema] ASC, [BusinessObjectName] ASC)
);




GO
CREATE TRIGGER [meta].[UC_BusinessObject_After_IU] ON [meta].[BusinessObject]
AFTER INSERT, UPDATE 
AS
BEGIN

	DECLARE @BusinessObjectSchema NVARCHAR(255);
	DECLARE @Message NVARCHAR(MAX);

	IF (ROWCOUNT_BIG() = 0)
	RETURN;

	UPDATE bo WITH (TABLOCK) SET
		bo.BusinessObjectSchema = CONVERT(NVARCHAR(255), ep.value)
	FROM [meta].[BusinessObject] AS bo
	JOIN inserted AS i ON bo.BusinessObjectID = i.BusinessObjectID
	JOIN sys.extended_properties AS ep WITH (NOLOCK) ON (ep.class = 0) AND ep.value = i.BusinessObjectSchema

END;
GO

CREATE TRIGGER [meta].[UC_BusinessObject_After_D] ON [meta].[BusinessObject]
INSTEAD OF DELETE 
AS
BEGIN

	DECLARE @BusinessObjectSchema NVARCHAR(255);	
	DECLARE @BusinessObjectName NVARCHAR(255);
	DECLARE @BusinessObjectID BIGINT;
	DECLARE @DWTransformStagingSchemaName NVARCHAR(255);
	DECLARE @ReferencingCount INT;

	SELECT @DWTransformStagingSchemaName = CAST(ep.value AS NVARCHAR(255))
	FROM sys.extended_properties AS ep WITH (NOLOCK) 
	WHERE (ep.class = 0) AND (ep.name = 'DWTransformStagingSchemaName')

	IF (ROWCOUNT_BIG() = 0) RETURN;

	BEGIN TRY 
		BEGIN TRANSACTION; 

		DECLARE Cur CURSOR LOCAL FOR
			SELECT d.BusinessObjectID, d.BusinessObjectSchema, d.BusinessObjectName
			FROM deleted AS d
		OPEN cur
		FETCH NEXT FROM cur INTO @BusinessObjectID, @BusinessObjectSchema, @BusinessObjectName
		WHILE (@@FETCH_STATUS = 0)
		BEGIN

			SELECT @ReferencingCount = COUNT(1)
			FROM sys.sql_expression_dependencies AS sed WITH (NOLOCK)
			JOIN sys.objects AS o WITH (NOLOCK) ON sed.referencing_id = o.object_id
			JOIN sys.schemas AS s WITH (NOLOCK) ON o.schema_id = s.schema_id
			JOIN deleted AS d ON (sed.referenced_schema_name IN (@BusinessObjectSchema)) AND (sed.referenced_entity_name = @BusinessObjectName)
			JOIN sys.extended_properties AS ep WITH (NOLOCK) ON (ep.major_id = OBJECT_ID(@DWTransformStagingSchemaName + '.' + @BusinessObjectName)) AND (ep.name = 'SourceObjectName')
			WHERE s.name IN ('Transform', @DWTransformStagingSchemaName) AND o.name != ep.value

			IF (@ReferencingCount > 0)
			BEGIN
				PRINT 'Unable to drop ' + QUOTENAME(@BusinessObjectSchema) + '.' + QUOTENAME(@BusinessObjectName) + ' due to unresolved dependencies'
				ROLLBACK TRANSACTION;
			--	RETURN;
			END
		
		END
		CLOSE cur
		DEALLOCATE cur

		COMMIT TRANSACTION; 
	END TRY 
	BEGIN CATCH
		IF (@@TRANCOUNT > 0) 
			ROLLBACK TRANSACTION; 

	END CATCH

END;