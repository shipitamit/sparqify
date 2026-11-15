a = LOAD 'in.csv' USING PigStorage(',') AS (id:int, amt:double, region:chararray);
b = FILTER a BY amt > 100;
c = GROUP b BY region;
d = FOREACH c GENERATE group, SUM(b.amt) AS total;
STORE d INTO 'out';
