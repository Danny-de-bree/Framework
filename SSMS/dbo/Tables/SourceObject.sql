CREATE TABLE [dbo].[SourceObject] (
    [SourceObjectID]       BIGINT         IDENTITY (1, 1) NOT NULL,
    [SourceConnectionID]   BIGINT         NOT NULL,
    [SourceObjectTable]    NVARCHAR (255) NOT NULL,
    [SourceSchema]         NVARCHAR (255) NOT NULL,
    [SourceTable]          NVARCHAR (255) NOT NULL,
    [LoadModeETL]          NVARCHAR (50)  CONSTRAINT [DF_SourceObject_LoadModeETL] DEFAULT ('FULL') NULL,
    [IsReset]              TINYINT        CONSTRAINT [DF_SourceObject_IsReset] DEFAULT ((1)) NULL,
    [IncrementalField]     NVARCHAR (255) CONSTRAINT [DF_SourceObject_IncrementalField] DEFAULT ('') NULL,
    [IncrementalOffSet]    INT            CONSTRAINT [DF_SourceObject_IncrementalOffSet] DEFAULT ((0)) NULL,
    [SourceObjectFilter]   NVARCHAR (255) CONSTRAINT [DF_SourceObject_SourceObjectFilter] DEFAULT ('') NULL,
    [IsEnabled]            TINYINT        CONSTRAINT [DF_SourceObject_IsEnabled] DEFAULT ((1)) NULL,
    [SourceObjectSchedule] NVARCHAR (255) CONSTRAINT [DF_SourceObject_SourceObjectSchedule] DEFAULT ('') NULL,
    [PreserveSCD2History]  TINYINT        CONSTRAINT [DF_SourceObject_PreserveSCD2History] DEFAULT ((0)) NULL,
    CONSTRAINT [PK_SourceObject] PRIMARY KEY NONCLUSTERED ([SourceObjectID] ASC) WITH (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON),
    CONSTRAINT [CK_SourceObject_LoadModeETL] CHECK ([LoadModeETL]='CDC' OR [LoadModeETL]='ICL' OR [LoadModeETL]='CUSTOM' OR [LoadModeETL]='CT' OR [LoadModeETL]='FULL'),
    CONSTRAINT [FK_SourceObject_SourceConnection] FOREIGN KEY ([SourceConnectionID]) REFERENCES [dbo].[SourceConnection] ([SourceConnectionID]),
    CONSTRAINT [UC_SourceObject] UNIQUE CLUSTERED ([SourceConnectionID] ASC, [SourceObjectTable] ASC)
);



