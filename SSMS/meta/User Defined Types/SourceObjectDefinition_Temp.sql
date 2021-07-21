CREATE TYPE [meta].[SourceObjectDefinition_Temp] AS TABLE (
    [SourceObjectID]                 BIGINT         NULL,
    [SourceConnectionID]             BIGINT         NULL,
    [SourceObjectColumnID]           BIGINT         NULL,
    [SourceObjectColumnName]         NVARCHAR (255) NULL,
    [SourceObjectColumnType]         NVARCHAR (255) NULL,
    [SourceObjectColumnLength]       NVARCHAR (128) NULL,
    [SourceObjectColumnIsNullable]   NVARCHAR (10)  NULL,
    [SourceObjectColumnIsPrimaryKey] BIT            NULL,
    [SourceObjectPrimaryKeyNumber]   TINYINT        NULL);

