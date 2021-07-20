CREATE FUNCTION dbo.ufnGetEasterSundayFromYear(@year INT)
RETURNS DATE
AS
BEGIN

	DECLARE @EasterSunday DATE;
	DECLARE @a INT;
	DECLARE @b INT;
	DECLARE @c INT;
	DECLARE @d INT;
	DECLARE @e INT;
	DECLARE @f INT;
	DECLARE @g INT;
	DECLARE @h INT;
	DECLARE @i INT;
	DECLARE @k INT;
	DECLARE @l INT;
	DECLARE @m INT;
	DECLARE @n INT;
	DECLARE @p INT;
	
	SET @a = @year % 19;
	SET @b = @year / 100;
	SET @c = @year % 100;
	SET @d = @b / 4;
	SET @e = @b % 4;
	SET @f = (@b + 8) / 25;
	SET @g = (@b - @f + 1) / 3;
	SET @h = (19 * @a + @b - @d - @g + 15) % 30;
	SET @i = @c / 4;
	SET @k = @c % 4;
	SET @l = (32 + 2 * @e + 2 * @i - @h - @k) % 7;
	SET @m = (@a + 11 * @h + 22 * @l) / 451;
	SET @n = (@h + @l - 7 * @m + 114) / 31;
	SET @p = (@h + @l - 7 * @m + 114) % 31 + 1;
	SET @EasterSunday = DATEFROMPARTS(@year, @n, @p);

	RETURN @EasterSunday
END