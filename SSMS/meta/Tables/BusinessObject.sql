CREATE TABLE [meta].[BusinessObject] (
    [BusinessObjectID]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [BusinessObjectSchema] NVARCHAR (255)  NOT NULL,
    [BusinessObjectName]   NVARCHAR (255)  NOT NULL,
    [LoadPattern]          NVARCHAR (50)   CONSTRAINT [DF_BusinessObject_LoadPattern] DEFAULT (N'FULL') NULL,
    [IncrementalField]     NVARCHAR (255)  CONSTRAINT [DF_BusinessObject_IncrementalField] DEFAULT (N'') NULL,
    [IncrementalOffSet]    INT             CONSTRAINT [DF_BusinessObject_IncrementalOffSet] DEFAULT ((0)) NULL,
    [LookupKey]            NVARCHAR (255)  CONSTRAINT [DF_BusinessObject_LookupKey] DEFAULT (N'') NULL,
    [RolePlayingEntity]    NVARCHAR (4000) CONSTRAINT [DF_BusinessObject_RolePlayingEntity] DEFAULT (N'') NULL,
    [Schedule]             NVARCHAR (255)  CONSTRAINT [DF_BusinessObject_Schedule] DEFAULT (N'') NULL,
    [PreserveSCD2History]  TINYINT         CONSTRAINT [DF_BusinessObject_PreserveSCD2History] DEFAULT ((0)) NULL,
    [IsEnabled]            TINYINT         CONSTRAINT [DF_BusinessObject_IsEnabled] DEFAULT ((1)) NULL,
    CONSTRAINT [PK_BusinessObject] PRIMARY KEY CLUSTERED ([BusinessObjectID] ASC) WITH (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON),
    CONSTRAINT [CC_BusinessObject_BusinessObjectSchema] CHECK ([meta].[ufnCheckSchema]([BusinessObjectSchema])=(1)),
    CONSTRAINT [UC_BusinessObject] UNIQUE NONCLUSTERED ([BusinessObjectSchema] ASC, [BusinessObjectName] ASC)
);


GO
CREATE TRIGGER [meta].[UC_BusinessObject_After_IU] ON [meta].[BusinessObject]
AFTER INSERT, UPDATE 
AS
BEGIN

	DECLARE @BusinessObjectSchema NVARCHAR(255);
	DECLARE @Message NVARCHAR(MAX);

	IF (ROWCOUNT_BIG() = 0)
	RETURN;

	UPDATE bo WITH (TABLOCK) SET
		bo.BusinessObjectSchema = CONVERT(NVARCHAR(255), ep.value)
	FROM [meta].[BusinessObject] AS bo
	JOIN inserted AS i ON bo.BusinessObjectID = i.BusinessObjectID
	JOIN sys.extended_properties AS ep WITH (NOLOCK) ON (ep.class = 0) AND ep.value = i.BusinessObjectSchema

END;