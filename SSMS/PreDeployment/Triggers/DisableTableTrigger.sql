
IF (SELECT t.is_disabled FROM sys.triggers AS t WHERE (t.name = 'TableTracking')) = 0 
DISABLE TRIGGER [TableTracking] ON DATABASE;