CREATE TABLE [meta].[BusinessObjectExecutionPlan] (
    [DestinationSchemaName]  NVARCHAR (255) NULL,
    [DestinationTableName]   NVARCHAR (255) NULL,
    [LoadSequence]           INT            NULL,
    [PrecedenceObjectSchema] NVARCHAR (255) NULL,
    [PrecedenceObjectName]   NVARCHAR (255) NULL,
    [ExecutionStatusCode]    INT            NULL,
    [ExecutionStatus]        NVARCHAR (255) NULL,
    [ScheduleOk]             TINYINT        NULL,
    [IsEnabled]              TINYINT        NULL,
    [MaxLoadSequence]        INT            NULL
);

