CREATE TABLE [meta].[SourceConnectionDataTypeMapping] (
	[SourceConnectionDataTypeMappingID] BIGINT IDENTITY(1,1) NOT NULL
,	[SourceConnectionID] BIGINT NOT NULL 
,	[SourceDataType] NVARCHAR(255) NOT NULL
,	[SourceDataTypeLength] INT NULL
,	[SourceDataTypeScale] INT NULL
,	[TargetDataType] NVARCHAR(255) NOT NULL
,	[TargetDataTypeLength] INT NULL
,	[TargetDataTypeScale] INT NULL
,	CONSTRAINT [PK_SourceConnectionDataTypeMapping] PRIMARY KEY CLUSTERED ([SourceConnectionDataTypeMappingID]) WITH (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON)
,	CONSTRAINT [UC_SourceConnectionDataTypeMapping] UNIQUE NONCLUSTERED ([SourceConnectionID], [SourceDataType])
,	CONSTRAINT [FK_SourceConnectionDataTypeMapping_SourceConnection] FOREIGN KEY([SourceConnectionID]) REFERENCES [meta].[SourceConnection] ([SourceConnectionID])
)