CREATE TABLE [dbo].[Log] (
    [Id]        INT            IDENTITY (1, 1) NOT NULL,
    [Subsystem] NVARCHAR (8)   NOT NULL,
    [Type]      NVARCHAR (8)   NOT NULL,
    [Severity]  INT            NOT NULL,
    [Source]    NVARCHAR (255) NULL,
    [Message]   NVARCHAR (MAX) NULL,
    [Time]      DATETIME       NULL,
    [Entity]    NVARCHAR (128) NULL,
    [Operation] NVARCHAR (8)   NULL,
    [Rows]      INT            NULL,
    CONSTRAINT [CK_Log_Type] CHECK ([Type]='Error' OR [Type]='Debug' OR [Type]='Warning' OR [Type]='Info')
);




GO
CREATE NONCLUSTERED INDEX [NC_Log_time]
    ON [dbo].[Log]([Time] ASC);


GO
CREATE CLUSTERED INDEX [PK_Log]
    ON [dbo].[Log]([Id] ASC) WITH (FILLFACTOR = 90, PAD_INDEX = ON);



