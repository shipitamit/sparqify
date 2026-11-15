SELECT e.ename, NVL(e.comm,0) AS comm, DECODE(e.deptno,10,'ACC',20,'RES','OTH') AS dept, TO_CHAR(e.hiredate,'YYYY-MM-DD') AS hd
FROM emp e WHERE ROWNUM <= 10 AND e.hiredate > ADD_MONTHS(SYSDATE,-12) ORDER BY e.sal DESC;
