data work.people;
  input id name $ amount joined :yymmdd10.;
  datalines;
1 Ann 10.5 2024-01-02
2 O'Brien . 2024-02-03
3 Cy 7 2024-03-04
;
run;
data csvin; infile '/data/in.csv' dlm=',' dsd firstobs=2; input a b $ c; run;
