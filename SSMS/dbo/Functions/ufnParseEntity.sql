CREATE FUNCTION [dbo].[ufnParseEntity](@Definition nvarchar(max))
RETURNS @Result TABLE (
	Id int IDENTITY(1,1)
	,LogicalId int
	,TypeId int
	,Type nvarchar(16) COLLATE DATABASE_DEFAULT
	,StartPos int
	,EndPos int
	,Definition nvarchar(max) COLLATE DATABASE_DEFAULT
	,SelectIndex int
	,FieldIndex int
	,Field nvarchar(128) COLLATE DATABASE_DEFAULT
	,Expression nvarchar(max) COLLATE DATABASE_DEFAULT
)
AS
BEGIN
	DECLARE @vbCrLf CHAR(2);
	DECLARE @tokenList TABLE (SortOrder int, TypeId int, Type nvarchar(16), Token nvarchar(16) UNIQUE, TokenPos int NULL);
	DECLARE @pos1 int;
	DECLARE @pos2 int;
	DECLARE @len int;
	DECLARE @sortOrder int
	DECLARE @token nvarchar(16)
	DECLARE @tokenPos int;
	DECLARE @startPos int;
	DECLARE @endPos int;
	DECLARE @wrkDefinition nvarchar(max);
	DECLARE @wrkDefinition2 nvarchar(max);
	DECLARE @loopCnt int;
	DECLARE @typeId int;
	DECLARE @type nvarchar(16);
	DECLARE @tokenCnt int;
	DECLARE @field nvarchar(128);
	DECLARE @expression nvarchar(max);
	DECLARE @selectIndex int;
	DECLARE @fieldIndex int;

	SET @vbCrLf = CHAR(13) + CHAR(10);

	-- BEGIN: First find technical (T-SQL) blocks
	DELETE @tokenList;
	INSERT INTO @tokenList(SortOrder, Token, TypeId, Type) VALUES
		  ( 1, 'WITH',				-1, 'WITH')
		, ( 2, ';WITH',				-1, 'WITH')
		, ( 3, 'SELECT',			02, 'SELECT')
		, ( 4, 'FROM',				03, 'FROM')
		, ( 5, 'INNER JOIN',		04, 'INNER JOIN')
		, ( 6, 'LEFT JOIN',			04, 'LEFT JOIN')
		, ( 7, 'LEFT OUTER JOIN',	04, 'LEFT OUTER JOIN')
		, ( 8, 'RIGHT JOIN',		04, 'RIGHT JOIN')
		, ( 9, 'RIGHT OUTER JOIN',	04, 'RIGHT OUTER JOIN')
		, (10, 'CROSS JOIN',		04, 'CROSS JOIN')
		, (11, 'FULL JOIN',			04, 'FULL JOIN')
		, (12, 'FULL OUTER JOIN',	04, 'FULL OUTER JOIN')
		, (13, 'JOIN',				04, 'JOIN')
		, (14, 'CROSS APPLY',		05, 'CROSS APPLY')
		, (15, 'OUTER APPLY',		05, 'OUTER APPLY')
		, (16, 'WHERE',				06, 'WHERE')
		, (17, 'GROUP BY',			07, 'GROUP BY')
		, (18, 'HAVING',			08, 'HAVING')
		, (19, 'ORDER BY',			09, 'ORDER BY')
		, (20, 'OPTION',			10, 'OPTION')
		, (21, 'UNION',				11, 'UNION')
		, (22, 'UNION ALL',			12, 'UNION ALL')
		;  

	/* Remove all comments made using /**/ */
	WHILE (CHARINDEX('/*',@definition) > 0)
	BEGIN
		SELECT @definition = STUFF(@definition, CHARINDEX('/*',@definition), CHARINDEX('*/',@definition) - CHARINDEX('/*',@definition) + 2, /*2 is the length of the search term */ '')
	END;

	/* Remove all comments made using -- */
	WHILE (CHARINDEX('--',@definition) > 0) AND (CHARINDEX(CHAR(13) + CHAR(10),@definition,CHARINDEX('--',@definition)) > CHARINDEX('--',@definition))
	BEGIN
		SELECT @definition = STUFF(@definition, CHARINDEX('--',@definition), CHARINDEX(@vbCrLf,@definition,CHARINDEX('--',@definition)) - CHARINDEX('--',@definition) + 2, '')
	END;

	/* Udate definition and remove blank lines */
	SET @Definition = REPLACE(TRIM(@definition), CHAR(13) + CHAR(13), '') 
	
	/* you can now search this without false positives from comments. */

	SET @wrkDefinition = @Definition;
	SET @loopCnt = 0;
	SET @startPos = 0;
	SET @endPos = 0;

	WHILE (@wrkDefinition <> '')
	BEGIN
		SET @loopCnt = @loopCnt + 1;

		UPDATE @tokenList SET TokenPos = NULL;

		UPDATE @tokenList SET TokenPos = meta.ufnFindSection(@wrkDefinition, Token, 1);

		-- Only for debugging
		DECLARE tokenC CURSOR FOR
		SELECT t.SortOrder, t.Token, t.TokenPos FROM @tokenList AS t;
		OPEN tokenC;
		FETCH NEXT FROM tokenC INTO @sortOrder, @token, @tokenPos;
		WHILE @@FETCH_STATUS = 0
		BEGIN
			FETCH NEXT FROM tokenC INTO @sortOrder, @token, @tokenPos;
		END;
		CLOSE tokenC;
		DEALLOCATE tokenC;

		-- Filter the redundant JOIN away if it is actual a INNER JOIN
		IF (SELECT TokenPos FROM @tokenList WHERE (Token = 'JOIN')) = (SELECT TokenPos + LEN('INNER') + 1 FROM @tokenList WHERE (Token = 'INNER JOIN'))
		BEGIN
			UPDATE @tokenList SET TokenPos = -1 WHERE (Token = 'JOIN');
		END;

		-- Filter the redundant JOIN away if it is actual a LEFT JOIN
		IF (SELECT TokenPos FROM @tokenList WHERE (Token = 'JOIN')) = (SELECT TokenPos + LEN('LEFT') + 1 FROM @tokenList WHERE (Token = 'LEFT JOIN'))
		BEGIN
			UPDATE @tokenList SET TokenPos = -1 WHERE (Token = 'JOIN');
		END;

		-- Filter the redundant JOIN away if it is actual a LEFT OUTER JOIN
		IF (SELECT TokenPos FROM @tokenList WHERE (Token = 'JOIN')) = (SELECT TokenPos + LEN('LEFT OUTER') + 1 FROM @tokenList WHERE (Token = 'LEFT OUTER JOIN'))
		BEGIN
			UPDATE @tokenList SET TokenPos = -1 WHERE (Token = 'JOIN');
		END;

		-- Filter the redundant JOIN away if it is actual a RIGHT JOIN
		IF (SELECT TokenPos FROM @tokenList WHERE (Token = 'JOIN')) = (SELECT TokenPos + LEN('RIGHT') + 1 FROM @tokenList WHERE (Token = 'RIGHT JOIN'))
		BEGIN
			UPDATE @tokenList SET TokenPos = -1 WHERE (Token = 'JOIN');
		END;

		-- Filter the redundant JOIN away if it is actual a RIGHT OUTER JOIN
		IF (SELECT TokenPos FROM @tokenList WHERE (Token = 'JOIN')) = (SELECT TokenPos + LEN('RIGHT OUTER') + 1 FROM @tokenList WHERE (Token = 'RIGHT OUTER JOIN'))
		BEGIN
			UPDATE @tokenList SET TokenPos = -1 WHERE (Token = 'JOIN');
		END;

		-- Filter the redundant JOIN away if it is actual a CROSS JOIN
		IF (SELECT TokenPos FROM @tokenList WHERE (Token = 'JOIN')) = (SELECT TokenPos + LEN('CROSS') + 1 FROM @tokenList WHERE (Token = 'CROSS JOIN'))
		BEGIN
			UPDATE @tokenList SET TokenPos = -1 WHERE (Token = 'JOIN');
		END;

		-- Filter the redundant UNION away if it is actual a UNION ALL
		IF (SELECT TokenPos FROM @tokenList WHERE (Token = 'UNION')) = (SELECT TokenPos FROM @tokenList WHERE (Token = 'UNION ALL'))
		BEGIN
			UPDATE @tokenList SET TokenPos = -1 WHERE (Token = 'UNION');
		END;

		-- Filter the redundant JOIN away if it is actual a FULL JOIN
		IF (SELECT TokenPos FROM @tokenList WHERE (Token = 'JOIN')) = (SELECT TokenPos + LEN('FULL') + 1 FROM @tokenList WHERE (Token = 'FULL JOIN'))
		BEGIN
			UPDATE @tokenList SET TokenPos = -1 WHERE (Token = 'JOIN');
		END;

		-- Filter the redundant JOIN away if it is actual a FULL OUTER JOIN
		IF (SELECT TokenPos FROM @tokenList WHERE (Token = 'JOIN')) = (SELECT TokenPos + LEN('FULL OUTER') + 1 FROM @tokenList WHERE (Token = 'FULL OUTER JOIN'))
		BEGIN
			UPDATE @tokenList SET TokenPos = -1 WHERE (Token = 'JOIN');
		END;

		-- Only for debugging
		DECLARE tokenC CURSOR FOR
		SELECT t.SortOrder, t.Token, t.TokenPos FROM @tokenList AS t;
		OPEN tokenC;
		FETCH NEXT FROM tokenC INTO @sortOrder, @token, @tokenPos;
		WHILE @@FETCH_STATUS = 0
		BEGIN
			FETCH NEXT FROM tokenC INTO @sortOrder, @token, @tokenPos;
		END;
		CLOSE tokenC;
		DEALLOCATE tokenC;

		UPDATE @tokenList SET TokenPos = NULL WHERE TokenPos = -1;

		SET @pos1 = NULL;
		SET @pos2 = NULL;
		SELECT @pos1 = t.TokenPos, @typeId = t.TypeId, @type = t.Type FROM
			(SELECT ROW_NUMBER() OVER (ORDER BY TokenPos) AS RowNum, * FROM @tokenList WHERE (TokenPos IS NOT NULL)) AS t WHERE t.RowNum = 1;
		SELECT @pos2 = t.TokenPos FROM
			(SELECT ROW_NUMBER() OVER (ORDER BY TokenPos) AS RowNum, * FROM @tokenList WHERE (TokenPos IS NOT NULL)) AS t WHERE t.RowNum = 2;

		SET @len = CASE
			WHEN @pos1 IS NULL THEN LEN(@wrkDefinition)
			WHEN @pos2 IS NULL THEN LEN(@wrkDefinition)
			ELSE @pos2 - @pos1
		END;

		IF (@pos1 IS NULL)
		BEGIN
			SET @pos1 = 1;
		END;

		SET @startPos = @startPos + @pos1;
		SET @endPos = @endPos + @pos1 + (@len - 1);

		INSERT INTO @Result(TypeId, Type, StartPos, EndPos, Definition) VALUES(@typeId, @type, @startPos, @endPos, SUBSTRING(@wrkDefinition, @pos1, @len));

		SET @wrkDefinition = SUBSTRING(@wrkDefinition, @pos1 + @len, LEN(@wrkDefinition));

		SET @startPos = @startPos + (@len - 1);
	END;
	-- END: First find technical (T-SQL) blocks

	-- BEGIN: We now have to handle the WITH section as it can contain multiple CTE's
	IF EXISTS(SELECT 1 FROM @Result AS r WHERE (r.Type = 'WITH'))
	BEGIN
		SELECT
			@typeId = r.TypeId
			,@type = r.Type
			,@startPos = r.StartPos
			,@endPos = r.EndPos
			,@wrkDefinition = r.Definition
		FROM @Result AS r
		WHERE (r.Type = 'WITH');

		DELETE FROM @Result WHERE (Type = 'WITH');

		SELECT @pos1 = meta.ufnFindSection(@wrkDefinition, ',', 0);

		WHILE (@pos1 > 0)
		BEGIN
			SET @wrkDefinition2 = SUBSTRING(@wrkDefinition, @pos1, LEN(@wrkDefinition));

			SET @wrkDefinition = SUBSTRING(@wrkDefinition, 1, @pos1 - 1);
			SET @endPos = @startPos + LEN(@wrkDefinition) - 1;
			
				INSERT INTO @Result(TypeId, Type, StartPos, EndPos, Definition) 
				VALUES(@typeId, @type, @startPos, @endPos, @wrkDefinition);
			
			SET @startPos = @endPos + 1;

			SET @wrkDefinition = @wrkDefinition2;

			SELECT @pos1 = meta.ufnFindSection(SUBSTRING(@wrkDefinition, 2, LEN(@wrkDefinition)), ',', 0);
			IF (@pos1 > 0)
			BEGIN
				SET @pos1 += 1;
			END;
		END;

		SET @endPos = @startPos + LEN(@wrkDefinition) - 1;
		INSERT INTO @Result(TypeId, Type, StartPos, EndPos, Definition) VALUES(@typeId, @type, @startPos, @endPos, @wrkDefinition);
	END;
	-- END: We now have to handle the WITH section as it can contain multiple CTE's

	-- BEGIN: Now lets us find the logical stuff except meta
	SET @tokenCnt = 0;
	WHILE (@tokenCnt < 3)
	BEGIN
		SET @tokenCnt = @tokenCnt + 1;

		DELETE @tokenList;

		SET @wrkDefinition = @Definition;
		SET @loopCnt = 0;
		SET @startPos = 0;
		SET @endPos = 0;

		WHILE (@wrkDefinition <> '')
		BEGIN
			SET @loopCnt = @loopCnt + 1;

			UPDATE @tokenList SET TokenPos = NULL;

			UPDATE @tokenList SET TokenPos = meta.ufnFindSection(@wrkDefinition, Token, 0);

			-- Only for debugging
			DECLARE tokenC CURSOR FOR
			SELECT t.SortOrder, t.Token, t.TokenPos FROM @tokenList AS t;
			OPEN tokenC;
			FETCH NEXT FROM tokenC INTO @sortOrder, @token, @tokenPos;
			WHILE @@FETCH_STATUS = 0
			BEGIN
				FETCH NEXT FROM tokenC INTO @sortOrder, @token, @tokenPos;
			END;
			CLOSE tokenC;
			DEALLOCATE tokenC;

			UPDATE @tokenList SET TokenPos = NULL WHERE TokenPos = -1;

			SET @tokenPos = NULL;
			SET @pos1 = NULL;
			SET @pos2 = NULL;
			SELECT @tokenPos = t.TokenPos, @typeId = t.TypeId, @type = t.Type FROM
				(SELECT ROW_NUMBER() OVER (ORDER BY TokenPos) AS RowNum, * FROM @tokenList WHERE (TokenPos IS NOT NULL)) AS t WHERE t.RowNum = 1;

			IF (@tokenPos IS NULL)
			BEGIN
				BREAK;
			END;

			SET @pos1 = (SELECT MAX(StartPos) FROM @Result WHERE (Type IN ('UNION', 'UNION ALL')) AND (StartPos < @tokenPos));
			SET @pos2 = (SELECT MIN(StartPos) FROM @Result WHERE (Type IN ('UNION', 'UNION ALL')) AND (StartPos > @tokenPos));

			IF (@pos1 IS NULL)
			BEGIN
				BREAK;
			END;

			SET @len = CASE
				WHEN @pos2 IS NULL THEN LEN(@wrkDefinition) - @pos1 + 1
				ELSE @pos2 - @pos1
			END;

			SET @startPos = @startPos + @pos1;
			SET @endPos = @startPos + (@len - 1);

			INSERT INTO @Result(TypeId, Type, StartPos, EndPos, Definition) 
			VALUES(@typeId, @type, @startPos, @endPos, SUBSTRING(@wrkDefinition, @pos1, @len));

			SET @wrkDefinition = SUBSTRING(@wrkDefinition, @pos1 + @len, LEN(@wrkDefinition));

			SET @startPos = @startPos + (@len - 1);
		END;
	END;
	-- END: Now lets us find the logical stuff except meta

	-- BEGIN: The model logical block is special
	SET @startPos = (SELECT TOP 1 r.StartPos FROM @Result AS r ORDER BY r.StartPos);
	SET @endPos = (SELECT TOP 1 r.StartPos FROM @Result AS r WHERE (r.TypeId >= 100) ORDER BY r.StartPos);

	IF (@startPos IS NOT NULL)
	BEGIN
		IF (@endPos IS NOT NULL)
		BEGIN
			SET @endPos = @endPos - 1;
		END
		ELSE BEGIN
			SET @endPos = LEN(@Definition);
		END;

		SET @len = @endPos - @startPos + 1

		INSERT INTO @Result(TypeId, Type, StartPos, EndPos, Definition) VALUES(101, 'model', @startPos, @endPos, SUBSTRING(@Definition, @startPos, @len));
	END;
	-- END: The model logical block is special

	-- Now lets us link the technical sections into the logical sections
	UPDATE r
	SET r.LogicalId = s.Id
	FROM @Result AS r
	CROSS JOIN @Result AS s
	WHERE (r.TypeId < 100)
		AND (s.TypeId >= 100)
		AND (r.StartPos >= s.StartPos)
		AND (r.EndPos <= s.EndPos);

	-- What is left will attached to the model section
	UPDATE r
	SET r.LogicalId = s.Id
	FROM @Result AS r
	CROSS JOIN @Result AS s
	WHERE (r.TypeId < 100)
		AND (r.LogicalId IS NULL)
		AND (s.Type = 'model');

	-- Let us split the output fields
	DECLARE outputC CURSOR FOR
	SELECT r.TypeId, r.Type, r.StartPos, r.EndPos, r.Definition FROM @Result AS r
	WHERE (r.Type = 'SELECT')
	ORDER BY r.StartPos;
	OPEN outputC;

	FETCH NEXT FROM outputC INTO @typeId, @type, @startPos, @endPos, @wrkDefinition;

	SET @selectIndex = 0;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @selectIndex = @selectIndex + 1;
		SET @fieldIndex = 0;

		-- Remove SELECT
		SELECT @pos1 = CHARINDEX('SELECT', @wrkDefinition);
		SET @wrkDefinition = SUBSTRING(@wrkDefinition, @pos1 + LEN('SELECT'), LEN(@wrkDefinition));
		SET @startPos = @startPos + @pos1 + LEN('SELECT') - 1;

		SELECT @pos1 = meta.ufnFindSection(@wrkDefinition, ',', 0);
		WHILE (@pos1 > 0)
		BEGIN
			SET @wrkDefinition2 = SUBSTRING(@wrkDefinition, @pos1, LEN(@wrkDefinition));

			SET @wrkDefinition = SUBSTRING(@wrkDefinition, 1, @pos1 - 1);
			SET @endPos = @startPos + LEN(@wrkDefinition) - 1;

			-- Strip leading ',' (comma) and whites
			WHILE (CHARINDEX(',', @wrkDefinition, 1) = 1) OR (CHARINDEX(' ', @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(9), @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(10), @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(13), @wrkDefinition, 1) = 1)
			BEGIN
				SET @wrkDefinition = SUBSTRING(@wrkDefinition, 2, LEN(@wrkDefinition));
				SET @startPos = @startPos + 1;
			END;

			-- Strip trailing whites
			SET @wrkDefinition = REVERSE(@wrkDefinition);
			WHILE (CHARINDEX(' ', @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(9), @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(10), @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(13), @wrkDefinition, 1) = 1)
			BEGIN
				SET @wrkDefinition = SUBSTRING(@wrkDefinition, 2, LEN(@wrkDefinition));
				SET @endPos = @endPos - 1;
			END;
			SET @wrkDefinition = REVERSE(@wrkDefinition);

			SET @field = NULL;
			SET @expression = NULL;
			-- Do we have ' = '
			SELECT @pos1 = meta.ufnFindSection(@wrkDefinition, '=', 1);
			IF (@pos1 > 0)
			BEGIN
				SET @field = LTRIM(RTRIM(SUBSTRING(@wrkDefinition, 1, @pos1 - 1))); 
				SET @expression = LTRIM(RTRIM(SUBSTRING(@wrkDefinition, @pos1 + LEN(' = ') + 1, LEN(@wrkDefinition))));
			END
			ELSE BEGIN
				-- No ' = '
				-- Find last '.'
				SELECT @pos1 = meta.ufnFindSection(REVERSE(@wrkDefinition), '.', 0);
				IF (@pos1 > 0)
				BEGIN
					SET @field = REVERSE(SUBSTRING(REVERSE(@wrkDefinition), 1, @pos1 - 1));
				END
				ELSE BEGIN
					SET @field = @wrkDefinition;
				END;
				SET @expression = @wrkDefinition;
			END;

			IF (@field IS NOT NULL)
			BEGIN
				-- Strip leading '[', ''' (single pling) and whites
				WHILE (CHARINDEX('[', @field, 1) <> 0) OR (CHARINDEX('''', @field, 1) = 1) OR (CHARINDEX(' ', @field, 1) = 1) OR (CHARINDEX(CHAR(9), @field, 1) = 1) OR (CHARINDEX(CHAR(10), @field, 1) = 1) OR (CHARINDEX(CHAR(13), @field, 1) = 1)
				BEGIN
					SET @field = SUBSTRING(@field, 2, LEN(@field));
				END;

				-- Strip trailing ']', ''' (single pling) and whites
				SET @field = REVERSE(@field);
				WHILE (CHARINDEX(']', @field, 1) = 1) OR (CHARINDEX('''', @field, 1) = 1) OR (CHARINDEX(' ', @field, 1) = 1) OR (CHARINDEX(CHAR(9), @field, 1) = 1) OR (CHARINDEX(CHAR(10), @field, 1) = 1) OR (CHARINDEX(CHAR(13), @field, 1) = 1)
				BEGIN
					SET @field = SUBSTRING(@field, 2, LEN(@field));
				END;
				SET @field = REVERSE(@field);
			END;

			SET @fieldIndex = @fieldIndex + 1;
			INSERT INTO @Result(TypeId, Type, StartPos, EndPos, Definition, SelectIndex, FieldIndex, Field, Expression) VALUES(201, 'OUTPUT', @startPos, @endPos, @wrkDefinition, @selectIndex, @fieldIndex, @field, @expression);
			SET @startPos = @endPos + 1;

			SET @wrkDefinition = @wrkDefinition2;

			SELECT @pos1 = meta.ufnFindSection(SUBSTRING(@wrkDefinition, 2, LEN(@wrkDefinition)), ',', 0);
			IF (@pos1 > 0)
			BEGIN
				SET @pos1 += 1;
			END;
		END;

		-- Strip leading ',' (comma) and whites
		WHILE (CHARINDEX(',', @wrkDefinition, 1) = 1) OR (CHARINDEX(' ', @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(9), @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(10), @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(13), @wrkDefinition, 1) = 1)
		BEGIN
			SET @wrkDefinition = SUBSTRING(@wrkDefinition, 2, LEN(@wrkDefinition));
			SET @startPos = @startPos + 1;
		END;

		-- Strip trailing whites
		SET @wrkDefinition = REVERSE(@wrkDefinition);
		WHILE (CHARINDEX(' ', @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(9), @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(10), @wrkDefinition, 1) = 1) OR (CHARINDEX(CHAR(13), @wrkDefinition, 1) = 1)
		BEGIN
			SET @wrkDefinition = SUBSTRING(@wrkDefinition, 2, LEN(@wrkDefinition));
			SET @endPos = @endPos - 1;
		END;
		SET @wrkDefinition = REVERSE(@wrkDefinition);

		SET @field = NULL;
		SET @expression = NULL;
		-- Do we have ' = '
		SELECT @pos1 = meta.ufnFindSection(@wrkDefinition, '=', 1);
		IF (@pos1 > 0)
		BEGIN
			SET @field = LTRIM(RTRIM(SUBSTRING(@wrkDefinition, 1, @pos1 - 1)));
			SET @expression = LTRIM(RTRIM(SUBSTRING(@wrkDefinition, @pos1 + LEN(' = ') + 1, LEN(@wrkDefinition))));
		END
		ELSE BEGIN
			-- No ' AS '
			-- Find last '.'
			SELECT @pos1 = meta.ufnFindSection(REVERSE(@wrkDefinition), '.', 0);
			IF (@pos1 > 0)
			BEGIN
				SET @field = REVERSE(SUBSTRING(REVERSE(@wrkDefinition), 1, @pos1 - 1));
			END
			ELSE BEGIN
				SET @field = @wrkDefinition;
			END;
			SET @expression = @wrkDefinition;
		END;

		IF (@field IS NOT NULL)
		BEGIN
			-- Strip leading '[', ''' (single pling) and whites
			WHILE (CHARINDEX('[', @field, 1) = 1) OR (CHARINDEX('''', @field, 1) = 1) OR (CHARINDEX(' ', @field, 1) = 1) OR (CHARINDEX(CHAR(9), @field, 1) = 1) OR (CHARINDEX(CHAR(10), @field, 1) = 1) OR (CHARINDEX(CHAR(13), @field, 1) = 1)
			BEGIN
				SET @field = SUBSTRING(@field, 2, LEN(@field));
			END;

			-- Strip trailing ']', ''' (single pling) and whites
			SET @field = REVERSE(@field);
			WHILE (CHARINDEX(']', @field, 1) = 1) OR (CHARINDEX('''', @field, 1) = 1) OR (CHARINDEX(' ', @field, 1) = 1) OR (CHARINDEX(CHAR(9), @field, 1) = 1) OR (CHARINDEX(CHAR(10), @field, 1) = 1) OR (CHARINDEX(CHAR(13), @field, 1) = 1)
			BEGIN
				SET @field = SUBSTRING(@field, 2, LEN(@field));
			END;
			SET @field = REVERSE(@field);
		END;

		SET @endPos = @startPos + LEN(@wrkDefinition) - 1;
		SET @fieldIndex = @fieldIndex + 1;
		INSERT INTO @Result(TypeId, Type, StartPos, EndPos, Definition, SelectIndex, FieldIndex, Field, Expression) VALUES(201, 'OUTPUT', @startPos, @endPos, @wrkDefinition, @selectIndex, @fieldIndex, @field, @expression);

		FETCH NEXT FROM outputC INTO @typeId, @type, @startPos, @endPos, @wrkDefinition;
	END;

	CLOSE outputC;
	DEALLOCATE outputC;

	RETURN;
END