CREATE TABLE [dbo].[SourcePartition] (
    [SourcePartitionID]   BIGINT IDENTITY (1, 1) NOT NULL,
    [SourceConnectionID]  BIGINT NOT NULL,
    [SourcePartitionCode] BIGINT NOT NULL,
    CONSTRAINT [PK_SourcePartition] PRIMARY KEY CLUSTERED ([SourcePartitionID] ASC) WITH (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON),
    CONSTRAINT [FK_SourcePartition_SourceConnection] FOREIGN KEY ([SourceConnectionID]) REFERENCES [dbo].[SourceConnection] ([SourceConnectionID])
);

