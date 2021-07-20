CREATE TABLE [dbo].[Settings] (
    [Namespace] NVARCHAR (64)  NOT NULL,
    [Scope]     NVARCHAR (128) NOT NULL,
    [Name]      NVARCHAR (20)  NOT NULL,
    [Value]     NVARCHAR (128) NOT NULL,
    [Type]      NVARCHAR (16)  NOT NULL,
    [State]     NVARCHAR (8)   NOT NULL,
    [Timestamp] DATETIME       NOT NULL
);


GO
CREATE UNIQUE CLUSTERED INDEX [C_Settings_1]
    ON [dbo].[Settings]([Namespace] ASC, [Name] ASC);

