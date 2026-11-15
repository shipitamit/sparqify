CREATE PROCEDURE dbo.usp_load_orders
    @run_date DATE,
    @min_amount DECIMAL(10,2) = 100,
    @rows INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @cnt INT = 0;
    DECLARE @label VARCHAR(20);

    SELECT @cnt = COUNT(*) FROM dbo.stg_orders WHERE order_date = @run_date;

    IF @cnt = 0
    BEGIN
        PRINT 'nothing to load';
        SET @rows = 0;
        RETURN;
    END
    ELSE
        SET @label = 'full';

    SELECT o.order_id, o.amount, ISNULL(c.region, 'n/a') AS region
    INTO #big
    FROM dbo.stg_orders o LEFT JOIN dbo.customers c ON c.id = o.customer_id
    WHERE o.order_date = @run_date AND o.amount >= @min_amount;

    BEGIN TRY
        DELETE FROM dbo.fact_orders WHERE order_date = @run_date;
        INSERT INTO dbo.fact_orders (order_id, amount, region, order_date)
        SELECT order_id, amount, region, @run_date FROM #big;
    END TRY
    BEGIN CATCH
        RAISERROR('load failed', 16, 1);
    END CATCH

    WHILE @cnt > 3
    BEGIN
        SET @cnt = @cnt - 1;
    END

    IF EXISTS (SELECT 1 FROM dbo.fact_orders WHERE amount < 0)
        PRINT 'negative amounts found';

    SET @rows = @cnt;
    DROP TABLE #big;
END
