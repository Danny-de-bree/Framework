CREATE TABLE [dbo].[SourceObjectKeyColumn] (
    [SourceObjectKeyColumnID]   BIGINT         IDENTITY (1, 1) NOT NULL,
    [SourceObjectID]            BIGINT         NOT NULL,
    [SourceObjectKeyColumnName] NVARCHAR (250) NOT NULL,
    CONSTRAINT [PK_SourceObjectKeyColumn] PRIMARY KEY CLUSTERED ([SourceObjectKeyColumnID] ASC),
    CONSTRAINT [FK_SourceObjectKeyColumn_SourceObject] FOREIGN KEY ([SourceObjectID]) REFERENCES [dbo].[SourceObject] ([SourceObjectID]) ON DELETE CASCADE
);

