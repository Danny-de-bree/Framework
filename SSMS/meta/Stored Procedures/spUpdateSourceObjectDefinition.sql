CREATE PROCEDURE [meta].[spUpdateSourceObjectDefinition] 
	
	@SourceObjectDefinition [meta].[SourceObjectDefinition_Temp] READONLY

AS 
BEGIN

	/* Merge update dbo.SourceObjectDefinition */

	MERGE [meta].[SourceObjectDefinition] AS [TARGET] 
	USING @SourceObjectDefinition AS [SOURCE]
	ON ([SOURCE].[SourceObjectID] = [TARGET].[SourceObjectID]) AND ([SOURCE].[SourceObjectColumnID] = [TARGET].[SourceObjectColumnID])

	WHEN MATCHED AND EXISTS (
		SELECT 
			[SOURCE].[SourceObjectID]				
		,	[SOURCE].[SourceConnectionID]			
		,	[SOURCE].[SourceObjectColumnID]			
		,	[SOURCE].[SourceObjectColumnName]		
		,	[SOURCE].[SourceObjectColumnType]		
		,	[SOURCE].[SourceObjectColumnLength]		
		,	[SOURCE].[SourceObjectColumnIsNullable]	
		,	[SOURCE].[SourceObjectColumnIsPrimaryKey]
		,	[SOURCE].[SourceObjectPrimaryKeyNumber]		

		EXCEPT 
 
		SELECT 
			[TARGET].[SourceObjectID]				
		,	[TARGET].[SourceConnectionID]			
		,	[TARGET].[SourceObjectColumnID]			
		,	[TARGET].[SourceObjectColumnName]		
		,	[TARGET].[SourceObjectColumnType]		
		,	[TARGET].[SourceObjectColumnLength]		
		,	[TARGET].[SourceObjectColumnIsNullable]	
		,	[TARGET].[SourceObjectColumnIsPrimaryKey]
		,	[TARGET].[SourceObjectPrimaryKeyNumber]	
	) 
	THEN UPDATE SET 
			[TARGET].[SourceObjectID]					=	[SOURCE].[SourceObjectID]				
		,	[TARGET].[SourceConnectionID]				=	[SOURCE].[SourceConnectionID]			
		,	[TARGET].[SourceObjectColumnID]				=	[SOURCE].[SourceObjectColumnID]			
		,	[TARGET].[SourceObjectColumnName]			=	[SOURCE].[SourceObjectColumnName]		
		,	[TARGET].[SourceObjectColumnType]			=	[SOURCE].[SourceObjectColumnType]		
		,	[TARGET].[SourceObjectColumnLength]			=	[SOURCE].[SourceObjectColumnLength]		
		,	[TARGET].[SourceObjectColumnIsNullable]		=	[SOURCE].[SourceObjectColumnIsNullable]	
		,	[TARGET].[SourceObjectColumnIsPrimaryKey]	=	[SOURCE].[SourceObjectColumnIsPrimaryKey]
		,	[TARGET].[SourceObjectPrimaryKeyNumber]		=	[SOURCE].[SourceObjectPrimaryKeyNumber]	
	
	WHEN NOT MATCHED THEN 
	INSERT (
		[SourceObjectID]				
	,	[SourceConnectionID]			
	,	[SourceObjectColumnID]			
	,	[SourceObjectColumnName]		
	,	[SourceObjectColumnType]		
	,	[SourceObjectColumnLength]		
	,	[SourceObjectColumnIsNullable]	
	,	[SourceObjectColumnIsPrimaryKey]
	,	[SourceObjectPrimaryKeyNumber]	
	)
	VALUES (
		[SOURCE].[SourceObjectID]				
	,	[SOURCE].[SourceConnectionID]			
	,	[SOURCE].[SourceObjectColumnID]			
	,	[SOURCE].[SourceObjectColumnName]		
	,	[SOURCE].[SourceObjectColumnType]		
	,	[SOURCE].[SourceObjectColumnLength]		
	,	[SOURCE].[SourceObjectColumnIsNullable]	
	,	[SOURCE].[SourceObjectColumnIsPrimaryKey]
	,	[SOURCE].[SourceObjectPrimaryKeyNumber]		
	)
	;

END;