CREATE TABLE [meta].[SourceObjectExecutionPlan] (
    [DestinationSchemaName]  NVARCHAR (255) NULL,
    [DestinationTableName]   NVARCHAR (255) NULL,
    [LoadSequence]           INT            NULL,
    [SourceObjectPrefix]     NVARCHAR (255) NULL,
    [PrecedenceObjectSchema] NVARCHAR (255) NULL,
    [PrecedenceObjectName]   NVARCHAR (255) NULL,
    [ExecutionStatusCode]    INT            NULL,
    [ExecutionStatus]        NVARCHAR (255) NULL,
    [ScheduleOk]             TINYINT        NULL,
    [DataSourceType]         NVARCHAR (255) NULL,
    [IsEnabled]              TINYINT        NULL,
    [MaxLoadSequence]        INT            NULL
);

