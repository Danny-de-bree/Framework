CREATE PROCEDURE [meta].[spLookUpLoadSequence]
AS 
BEGIN
	
	DECLARE @DefaultMaxDop INT;
	DECLARE @RowCounter INT = 1;

	DROP TABLE IF EXISTS #LoadSequence;
	CREATE TABLE #LoadSequence (LoadSequence INT);

	SELECT @DefaultMaxDop = CONVERT(INT, [value]) 
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.name = 'DefaultMaxDop');

	WHILE (@RowCounter <= @DefaultMaxDop)
	BEGIN

		INSERT INTO #LoadSequence (LoadSequence)
		SELECT @RowCounter AS LoadSequence 

		SET @RowCounter = @RowCounter + 1;
	END;

	/* Get list of load sequences from #temp table */
	SELECT [LoadSequence]
	FROM #LoadSequence;

END;
