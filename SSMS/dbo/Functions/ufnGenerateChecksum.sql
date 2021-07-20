
CREATE FUNCTION [dbo].[ufnGenerateChecksum](@input [nvarchar](max)) 
RETURNS [bigint]
BEGIN
	DECLARE @subStrLength [int]
	DECLARE @retval [bigint]
	DECLARE @inputPos [int]
	DECLARE @inputLenght [int]

	-- Convert to upper
	SET @input = UPPER(@input)

	-- Needed as stored entities can be switched between DW and OLTP mode
	SET @input = REPLACE(@input, 'DW.', '[OLTP].')
	SET @input = REPLACE(@input, '[DW].', '[OLTP].')
	SET @input = REPLACE(@input, 'OLTP.', '[OLTP].')

	-- Also they can be switched between DM and MODEL mode
	SET @input = REPLACE(@input, 'DM.', '[MODEL].')
	SET @input = REPLACE(@input, '[DM].', '[MODEL].')
	SET @input = REPLACE(@input, 'MODEL.', '[MODEL].')

	SET @retval = 0
	SET @inputPos = 1
	SET @inputLenght = LEN(@input)

	WHILE (@inputPos <= @inputLenght)
	BEGIN
		IF ((@inputLenght - (@inputPos - 1)) > 4000)
		BEGIN
			SET @subStrLength = 4000
		END
		ELSE BEGIN
			SET @subStrLength = (@inputLenght - (@inputPos - 1))
		END

		IF (@retval = 0)
		BEGIN
			SET @retval = CONVERT(bigint, HASHBYTES('SHA2_256', SUBSTRING(@input, @inputPos, @subStrLength)))
		END
		ELSE BEGIN
			SET @retval = (@retval / 2) + (CONVERT(bigint, HASHBYTES('SHA2_256', SUBSTRING(@input, @inputPos, @subStrLength))) / 2)
		END

		SET @inputPos = @inputPos + @subStrLength
	END

	RETURN @retval
END