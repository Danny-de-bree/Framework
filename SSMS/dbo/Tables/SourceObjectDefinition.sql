CREATE TABLE [dbo].[SourceObjectDefinition] (
    [SourceObjectDefinitionID]       BIGINT         IDENTITY (1, 1) NOT NULL,
    [SourceObjectID]                 BIGINT         NOT NULL,
    [SourceConnectionID]             BIGINT         NOT NULL,
    [SourceObjectColumnID]           BIGINT         NOT NULL,
    [SourceObjectColumnName]         NVARCHAR (255) NOT NULL,
    [SourceObjectColumnType]         NVARCHAR (255) NOT NULL,
    [SourceObjectColumnLength]       NVARCHAR (128) NOT NULL,
    [SourceObjectColumnIsNullable]   NVARCHAR (10)  NOT NULL,
    [SourceObjectColumnIsPrimaryKey] BIT            NOT NULL,
    [SourceObjectPrimaryKeyNumber]   TINYINT        NOT NULL,
    CONSTRAINT [PK_SourceObjectDefinition] PRIMARY KEY NONCLUSTERED ([SourceObjectDefinitionID] ASC) WITH (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON),
    CONSTRAINT [FK_SourceObjectDefinition_SourceConnection] FOREIGN KEY ([SourceConnectionID]) REFERENCES [dbo].[SourceConnection] ([SourceConnectionID]),
    CONSTRAINT [FK_SourceObjectDefinition_SourceObject] FOREIGN KEY ([SourceObjectID]) REFERENCES [dbo].[SourceObject] ([SourceObjectID]) ON DELETE CASCADE,
    CONSTRAINT [UC_SourceObjectDefinition] UNIQUE CLUSTERED ([SourceObjectID] ASC, [SourceObjectColumnID] ASC)
);

