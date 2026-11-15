CREATE PROCEDURE dbo.usp_load @d DATE, @min INT AS
BEGIN
  DECLARE @n INT = 0;
  IF @min > 5 BEGIN SET @n = 1; END ELSE SET @n = 2;
  INSERT INTO tgt SELECT * FROM src WHERE dt = @d AND amt > @min;
  WHILE @n < 3 BEGIN SET @n = @n + 1; END
END
