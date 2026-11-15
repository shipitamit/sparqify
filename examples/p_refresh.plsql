CREATE OR REPLACE PROCEDURE refresh_totals(p_region IN VARCHAR2, p_days IN NUMBER DEFAULT 7, p_out OUT NUMBER) IS
  v_total NUMBER := 0;
  v_n     NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_n FROM sales WHERE region = p_region;
  IF v_n = 0 THEN
    DBMS_OUTPUT.PUT_LINE('no sales');
    RETURN;
  ELSIF v_n > 1000 THEN
    v_total := v_n * 2;
  ELSE
    v_total := v_n;
  END IF;

  FOR i IN 1..p_days LOOP
    UPDATE totals SET amount = amount + v_total WHERE region = p_region AND day_no = i;
  END LOOP;

  MERGE INTO region_totals t USING (SELECT region, SUM(amount) a FROM sales GROUP BY region) s
    ON (t.region = s.region)
    WHEN MATCHED THEN UPDATE SET t.amount = s.a
    WHEN NOT MATCHED THEN INSERT (region, amount) VALUES (s.region, s.a);

  FOR rec IN (SELECT region, amount FROM region_totals WHERE amount > 100) LOOP
    DBMS_OUTPUT.PUT_LINE(rec.region);
  END LOOP;
  p_out := v_total;
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    RAISE_APPLICATION_ERROR(-20001, 'refresh failed');
END refresh_totals;
/
