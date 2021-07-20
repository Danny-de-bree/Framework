CREATE TABLE [dbo].[CubeEntities] (
    [ID]               INT            IDENTITY (1, 1) NOT NULL,
    [EntityName]       NVARCHAR (255) NULL,
    [TableDefinition]  NVARCHAR (MAX) NULL,
    [ColumnDefinition] NVARCHAR (MAX) NULL,
    [State]            NVARCHAR (255) NULL,
    [Owner]            NVARCHAR (255) NULL,
    CONSTRAINT [PK_CubeEntities] PRIMARY KEY CLUSTERED ([ID] ASC)
);

