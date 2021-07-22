CREATE VIEW [meta].[TabularObjectView]
AS
WITH ExtendedProperties AS (
	SELECT 
		[AASOlapRegion]		=	MAX(CASE WHEN ep.name = 'AASOlapRegion' THEN ep.value ELSE '' END)
	,	[AASOlapServer]		=	MAX(CASE WHEN ep.name = 'AASOlapServer' THEN ep.value ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0)
)

SELECT 
	[AASOlapRegion]			=	CAST(ISNULL(x.AASOlapRegion, ep.AASOlapRegion) AS NVARCHAR(255))
,	[AASOlapServer]			=	CAST(ISNULL(x.AASOlapServer, ep.AASOlapServer) AS NVARCHAR(255))
,	[TabularObjectName]		=	CAST(x.ModelName AS NVARCHAR(255))
,	[IsEnabled]				=	CAST(x.IsEnabled AS TINYINT)
,	[IsIcremental]			=	CAST(x.IsIcremental AS TINYINT) 
FROM (
	VALUES 
		('Finance',		1, 0, NULL, NULL)
	,	('Production',	1, 0, NULL, NULL)
	,	('Inventory',	1, 0, NULL, NULL)
	,	('Sales',		1, 0, NULL, NULL)
	,	('Project',		1, 0, NULL, NULL)
	,	('Procurement', 1, 0, NULL, NULL)
	,	('HR',			1, 0, NULL, NULL)
	,	('Tracing',		1, 0, NULL, NULL)
	,	('Logistics',	1, 0, NULL, NULL)
	,	('DummyModel',	0, 0, NULL, NULL)
) AS x (ModelName, IsEnabled, IsIcremental, AASOlapRegion, AASOlapServer)
CROSS APPLY ExtendedProperties AS ep