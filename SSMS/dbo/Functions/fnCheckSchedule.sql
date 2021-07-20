-- ===========================================================================
-- Author:		Ivan Erik Kragh
-- Create date: 2016-09-15
-- Description:	Check if it is time to execute
-- ===========================================================================
/*
	@schedule:
	1) Daily							: More than one day since executed
	2) Daily After 15:30				: More than one day since executed and after 15:30
	3) Daily Before 04:30				: More than one day since executed and before 04:30
	4) Daily Between 22:10 and 22:50	: More than one day since executed and current time >= 22:10 and current time <= 22:50
	5) Daily Between 23:00 and 01:00	: More than one day since executed and current time >= 23:00 and current time <= 01:00 (next day)

	11) Hourly							: More than one hour since executed
	12) Hourly After 30					: More than one hour since executed and with current minute > 30
	13) Hourly Before 30				: More than one hour since executed and with current minute < 30
	14) Hourly Between 10 and 50		: More than one hour since executed and current minute >= 10 and current time <= 50
	15) Hourly Between 50 and 10		: More than one hour since executed and current minute >= 50 and current minute <= 10 (next hour)

	21) Every 10 minutes				: More than 10 minutes since executed
	21) Every 2 hour					: More than 2 Hours since executed

	Schedule is NULL or <blank>			: Return 1
	Schedule is wrong					: Return 0
	lastExecuted is NULL				: Return 1
*/
CREATE FUNCTION [dbo].[fnCheckSchedule](@schedule nvarchar(64), @lastExecuted datetime)
RETURNS @scheduleTable TABLE (ScheduleOk INT)
WITH SCHEMABINDING
AS
BEGIN
	DECLARE @workStr nvarchar(64);
	DECLARE @workStr2 nvarchar(64);
	DECLARE @scheduleOK int = 0;
	DECLARE @pos int;
	DECLARE @pos2 int;
	DECLARE @periodToken nvarchar(64);
	DECLARE @qualifier nvarchar(64);
	DECLARE @quantifier int;
	DECLARE @hourStr nvarchar(64);
	DECLARE @fromHour int;
	DECLARE @fromMinute int;
	DECLARE @toHour int;
	DECLARE @toMinute int;

	SET @schedule = LTRIM(RTRIM(@schedule));

	SET @workStr = @schedule;

	IF (@lastExecuted IS NULL) OR (@schedule = '') OR (@schedule IS NULL)
	BEGIN
		SET @scheduleOK = 1;
	END
	ELSE BEGIN
		SET @pos = CHARINDEX(' ', @workStr + ' '); -- Add space so we know that there always is one to find
		SET @periodToken = SUBSTRING(@workStr, 1, @pos - 1);
		SET @workStr = SUBSTRING(@workStr, @pos + 1, LEN(@workStr));
		SET @workStr = LTRIM(RTRIM(@workStr));

		IF (@periodToken IN ('Daily', 'Hourly'))
		BEGIN
			-- Find qualifier if they exists
			SET @pos = CHARINDEX(' ', @workStr + ' '); -- Add space so we know that there always is one to find
			SET @qualifier = SUBSTRING(@workStr, 1, @pos - 1);
			SET @workStr = SUBSTRING(@workStr, @pos + 1, LEN(@workStr));
			SET @workStr = LTRIM(RTRIM(@workStr));

			SET @qualifier = LTRIM(RTRIM(@qualifier));
			IF (@qualifier = '')
			BEGIN
				-- 1) Daily: More than one day since executed 
				IF (@periodToken = 'Daily')
				BEGIN
					IF (GETDATE() > DATEADD(DAY, 1, @lastExecuted))
					BEGIN
						SET @scheduleOK = 1;
					END;
				END
				-- 11) Hourly: More than one hour since executed
				ELSE IF (@periodToken = 'Hourly')
				BEGIN
					IF (GETDATE() > DATEADD(HOUR, 1, @lastExecuted))
					BEGIN
						SET @scheduleOK = 1;
					END;
				END;
			END
			ELSE IF (@qualifier = 'After')
			BEGIN
				-- 2) Daily After 15:30: More than one day since executed and after 15:30
				IF (@periodToken = 'Daily')
				BEGIN
					SET @pos = CHARINDEX(':', @workStr); -- Add space so we know that there always is one to find
					IF (@pos > 0)
					BEGIN
						SET @hourStr = SUBSTRING(@workStr, 1, @pos - 1);
						SET @hourStr = LTRIM(RTRIM(@hourStr));

						SET @workStr = SUBSTRING(@workStr, @pos + 1, LEN(@workStr));
						SET @workStr = LTRIM(RTRIM(@workStr));

						-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
						IF (@hourStr NOT LIKE '%[^0-9]%')
						BEGIN
							-- it is a number
							SET @fromHour = CAST(@hourStr AS int);
						END
						ELSE BEGIN
							SET @fromHour = NULL;
						END;

						-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
						IF (@workStr NOT LIKE '%[^0-9]%')
						BEGIN
							-- it is a number
							SET @fromMinute = CAST(@workStr AS int);
						END
						ELSE BEGIN
							SET @fromMinute = NULL;
						END;

						IF (@fromHour IS NOT NULL) AND (@fromMinute IS NOT NULL)
						BEGIN
							IF (GETDATE() > DATEADD(DAY, 1, @lastExecuted))
							BEGIN
								IF (
									(DATEPART(HOUR, GETDATE()) = @fromHour)
									AND (DATEPART(MINUTE, GETDATE()) > @fromMinute)
								)
								OR (
									(DATEPART(HOUR, GETDATE()) > @fromHour)
								)
								BEGIN
									SET @scheduleOK = 1;
								END;
							END;
						END;
					END;
				END
				-- 12) Hourly After 30: More than one hour since executed and with current minute > 30
				ELSE IF (@periodToken = 'Hourly')
				BEGIN
					-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
					IF (@workStr NOT LIKE '%[^0-9]%')
					BEGIN
						-- it is a number
						SET @fromMinute = CAST(@workStr AS int);
					END
					ELSE BEGIN
						SET @fromMinute = NULL;
					END;

					IF (@fromMinute IS NOT NULL)
					BEGIN
						IF (GETDATE() > DATEADD(HOUR, 1, @lastExecuted))
						BEGIN
							IF (DATEPART(MINUTE, GETDATE()) > @fromMinute)
							BEGIN
								SET @scheduleOK = 1;
							END;
						END;
					END;
				END;
			END
			ELSE IF (@qualifier = 'Before')
			BEGIN
				-- 3) Daily Before 04:30: More than one day since executed and before 04:30
				IF (@periodToken = 'Daily')
				BEGIN
					SET @pos = CHARINDEX(':', @workStr); -- Add space so we know that there always is one to find
					IF (@pos > 0)
					BEGIN
						SET @hourStr = SUBSTRING(@workStr, 1, @pos - 1);
						SET @hourStr = LTRIM(RTRIM(@hourStr));

						SET @workStr = SUBSTRING(@workStr, @pos + 1, LEN(@workStr));
						SET @workStr = LTRIM(RTRIM(@workStr));

						-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
						IF (@hourStr NOT LIKE '%[^0-9]%')
						BEGIN
							-- it is a number
							SET @toHour = CAST(@hourStr AS int);
						END
						ELSE BEGIN
							SET @toHour = NULL;
						END;

						-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
						IF (@workStr NOT LIKE '%[^0-9]%')
						BEGIN
							-- it is a number
							SET @toMinute = CAST(@workStr AS int);
						END
						ELSE BEGIN
							SET @toMinute = NULL;
						END;

						IF (@toHour IS NOT NULL) AND (@toMinute IS NOT NULL)
						BEGIN
							IF (GETDATE() > DATEADD(DAY, 1, @lastExecuted))
							BEGIN
								IF (
									(DATEPART(HOUR, GETDATE()) = @toHour)
									AND (DATEPART(MINUTE, GETDATE()) < @toMinute)
								)
								OR (
									(DATEPART(HOUR, GETDATE()) < @toHour)
								)
								BEGIN
									SET @scheduleOK = 1;
								END;
							END;
						END;
					END;
				END
				-- 13) Hourly Before 30: More than one hour since executed and with current minute < 30
				ELSE IF (@periodToken = 'Hourly')
				BEGIN
					-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
					IF (@workStr NOT LIKE '%[^0-9]%')
					BEGIN
						-- it is a number
						SET @toMinute = CAST(@workStr AS int);
					END
					ELSE BEGIN
						SET @toMinute = NULL;
					END;

					IF (@toMinute IS NOT NULL)
					BEGIN
						IF (GETDATE() > DATEADD(HOUR, 1, @lastExecuted))
						BEGIN
							IF (DATEPART(MINUTE, GETDATE()) < @toMinute)
							BEGIN
								SET @scheduleOK = 1;
							END;
						END;
					END;
				END;
			END
			ELSE IF (@qualifier = 'Between')
			BEGIN
				-- Let us find the 'and' and the second part
				SET @pos = CHARINDEX(' and ', @workStr);
				IF (@pos > 1)
				BEGIN
					SET @workStr2 = SUBSTRING(@workStr, @pos + LEN(' and '), LEN(@workStr));
					SET @workStr2 = LTRIM(RTRIM(@workStr2));

					SET @workStr = SUBSTRING(@workStr, 1, @pos - 1);
					SET @workStr = LTRIM(RTRIM(@workStr));

					-- 4) Daily Between 22:10 and 22:50: More than one day since executed and current time >= 22:10 and current time <= 22:50
					-- 5) Daily Between 23:00 and 01:00: More than one day since executed and current time >= 23:00 and current time <= 01:00 (next day)
					IF (@periodToken = 'Daily')
					BEGIN
						SET @pos = CHARINDEX(':', @workStr); -- Add space so we know that there always is one to find
						SET @pos2 = CHARINDEX(':', @workStr2); -- Add space so we know that there always is one to find
						IF (@pos > 0) AND (@pos2 > 0)
						BEGIN
							SET @hourStr = SUBSTRING(@workStr, 1, @pos - 1);
							SET @hourStr = LTRIM(RTRIM(@hourStr));

							SET @workStr = SUBSTRING(@workStr, @pos + 1, LEN(@workStr));
							SET @workStr = LTRIM(RTRIM(@workStr));

							-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
							IF (@hourStr NOT LIKE '%[^0-9]%')
							BEGIN
								-- it is a number
								SET @fromHour = CAST(@hourStr AS int);
							END
							ELSE BEGIN
								SET @fromHour = NULL;
							END;

							-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
							IF (@workStr NOT LIKE '%[^0-9]%')
							BEGIN
								-- it is a number
								SET @fromMinute = CAST(@workStr AS int);
							END
							ELSE BEGIN
								SET @fromMinute = NULL;
							END;

							SET @hourStr = SUBSTRING(@workStr2, 1, @pos - 1);
							SET @hourStr = LTRIM(RTRIM(@hourStr));

							SET @workStr2 = SUBSTRING(@workStr2, @pos + 1, LEN(@workStr));
							SET @workStr2 = LTRIM(RTRIM(@workStr2));

							-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
							IF (@hourStr NOT LIKE '%[^0-9]%')
							BEGIN
								-- it is a number
								SET @toHour = CAST(@hourStr AS int);
							END
							ELSE BEGIN
								SET @toHour = NULL;
							END;

							-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
							IF (@workStr2 NOT LIKE '%[^0-9]%')
							BEGIN
								-- it is a number
								SET @toMinute = CAST(@workStr2 AS int);
							END
							ELSE BEGIN
								SET @toMinute = NULL;
							END;

							IF (@fromHour IS NOT NULL) AND (@fromMinute IS NOT NULL) AND (@toHour IS NOT NULL) AND (@toMinute IS NOT NULL)
							BEGIN
								-- 4) Daily Between 22:10 and 22:50: More than one day since executed and current time >= 22:10 and current time <= 22:50
								IF (@fromHour < @toHour) OR ((@fromHour = @toHour) AND (@fromMinute < @toMinute))
								BEGIN
									IF (GETDATE() > DATEADD(DAY, 1, @lastExecuted))
									BEGIN
										IF (
											(
												(DATEPART(HOUR, GETDATE()) = @fromHour)
												AND (DATEPART(MINUTE, GETDATE()) >= @fromMinute)
											)
											OR (
												(DATEPART(HOUR, GETDATE()) > @fromHour)
											)
										)
										AND (
											(
												(DATEPART(HOUR, GETDATE()) = @toHour)
												AND (DATEPART(MINUTE, GETDATE()) <= @toMinute)
											)
											OR (
												(DATEPART(HOUR, GETDATE()) <= @toHour)
											)
										)
										BEGIN
											SET @scheduleOK = 1;
										END;
									END;
								END
								-- 5) Daily Between 23:00 and 01:00: More than one day since executed and current time >= 23:00 and current time <= 01:00 (next day)
								ELSE BEGIN
									IF (GETDATE() > DATEADD(DAY, 1, @lastExecuted))
									BEGIN
										IF (
											(
												(DATEPART(HOUR, GETDATE()) = @fromHour)
												AND (DATEPART(MINUTE, GETDATE()) >= @fromMinute)
											)
											OR (
												(DATEPART(HOUR, GETDATE()) > @fromHour)
											)
										)
										OR (
											(
												(DATEPART(HOUR, GETDATE()) = @toHour)
												AND (DATEPART(MINUTE, GETDATE()) <= @toMinute)
											)
											OR (
												(DATEPART(HOUR, GETDATE()) < @toHour)
											)
										)
										BEGIN
											SET @scheduleOK = 1;
										END;
									END;
								END;
							END;
						END;
					END
					-- 14) Hourly Between 10 and 50: More than one hour since executed and current minute >= 10 and current time <= 50
					-- 15) Hourly Between 50 and 10: More than one hour since executed and current minute >= 50 and current minute <= 10 (next hour)
					ELSE IF (@periodToken = 'Hourly')
					BEGIN
						-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
						IF (@workStr2 NOT LIKE '%[^0-9]%')
						BEGIN
							-- it is a number
							SET @toMinute = CAST(@workStr2 AS int);
						END
						ELSE BEGIN
							SET @toMinute = NULL;
						END;

						-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
						IF (@workStr NOT LIKE '%[^0-9]%')
						BEGIN
							-- it is a number
							SET @fromMinute = CAST(@workStr AS int);
						END
						ELSE BEGIN
							SET @fromMinute = NULL;
						END;

						IF (@fromMinute IS NOT NULL) AND (@toMinute IS NOT NULL)
						BEGIN
							-- 14) Hourly Between 10 and 50: More than one hour since executed and current minute >= 10 and current time <= 50
							IF (@fromMinute < @toMinute)
							BEGIN
								IF (GETDATE() > DATEADD(HOUR, 1, @lastExecuted))
								BEGIN
									IF (DATEPART(MINUTE, GETDATE()) >= @fromMinute)
										AND (DATEPART(MINUTE, GETDATE()) <= @toMinute)
									BEGIN
										SET @scheduleOK = 1;
									END;
								END;
							END
							-- 15) Hourly Between 50 and 10: More than one hour since executed and current minute >= 50 and current minute <= 10 (next hour)
							ELSE BEGIN
								IF (GETDATE() > DATEADD(HOUR, 1, @lastExecuted))
								BEGIN
									IF (
										(DATEPART(MINUTE, GETDATE()) >= @fromMinute)
										OR (DATEPART(MINUTE, GETDATE()) <= @toMinute)
									)
									BEGIN
										SET @scheduleOK = 1;
									END;
								END;
							END;
						END;
					END;
				END;
			END;
		END;
		ELSE IF (@periodToken = 'Every')
		BEGIN
			-- Find quantifier and qualifier
			SET @pos = CHARINDEX(' ', @workStr + ' '); -- Add space so we know that there always is one to find
			SET @workStr2 = SUBSTRING(@workStr, @pos + 1, LEN(@workStr));
			SET @workStr = SUBSTRING(@workStr, 1, @pos - 1);
			SET @qualifier = LTRIM(RTRIM(@workStr2));

			IF (LOWER(@qualifier) = 'minutes')
			BEGIN
				-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
				IF (@workStr NOT LIKE '%[^0-9]%')
				BEGIN
					-- it is a number
					SET @quantifier = CAST(@workStr AS int);
				END
				ELSE BEGIN
					SET @quantifier = NULL;
				END;

				IF (@quantifier IS NOT NULL)
				BEGIN
					IF (DATEDIFF(MINUTE, @lastExecuted, GETDATE()) >= @quantifier)
					BEGIN
						SET @scheduleOK = 1;
					END;
				END;
			END ELSE 

			IF (LOWER(@qualifier) = 'hour')
			BEGIN
				-- Must use this and not TRY_PARSE, ... as it must work on 2008R2
				IF (@workStr NOT LIKE '%[^0-9]%')
				BEGIN
					-- it is a number
					SET @quantifier = CAST(@workStr AS int);
				END
				ELSE BEGIN
					SET @quantifier = NULL;
				END;

				IF (@quantifier IS NOT NULL)
				BEGIN
					IF (DATEDIFF(HOUR, @lastExecuted, GETDATE()) >= @quantifier)
					BEGIN
						SET @scheduleOK = 1;
					END;
				END;
			END;
		END;
	END;

	INSERT INTO @scheduleTable (ScheduleOk)
	SELECT @scheduleOK

	RETURN;
END