
IF (SELECT t.is_disabled FROM sys.triggers AS t WHERE (t.name = 'ViewTracking')) = 0 
DISABLE TRIGGER [ViewTracking] ON DATABASE;