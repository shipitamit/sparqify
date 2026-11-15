CREATE OR REPLACE PROCEDURE load_t (p_d IN DATE, p_min IN NUMBER) AS
  v_n NUMBER := 0;
BEGIN
  IF p_min > 5 THEN v_n := 1; ELSE v_n := 2; END IF;
  INSERT INTO tgt SELECT * FROM src WHERE dt = p_d AND amt > p_min;
  FOR i IN 1..3 LOOP v_n := v_n + i; END LOOP;
END;
