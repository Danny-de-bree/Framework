CREATE TABLE [meta].[SourceConnection] (
    [SourceConnectionID]     BIGINT         IDENTITY (1, 1) NOT NULL,
    [DataSourceName]         NVARCHAR (255) NOT NULL,
    [DataSourceServerName]   NVARCHAR (255) NOT NULL,
    [DataSourceDatabaseName] NVARCHAR (255) NOT NULL,
    [DataSourceType]         NVARCHAR (255) NOT NULL,
    [DataSourceSchedule]     NVARCHAR (64)  CONSTRAINT [DF_DataSourceSchedule] DEFAULT ('Hourly') NULL,
    [SourceConnectionSchema] NVARCHAR (255) CONSTRAINT [DF_SourceConnection_SourceConnectionSchema] DEFAULT ('') NOT NULL,
    CONSTRAINT [PK_SourceConnection] PRIMARY KEY NONCLUSTERED ([SourceConnectionID] ASC) WITH (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON)
);

