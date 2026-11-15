proc sort data=a; by id; run;
proc sort data=b; by id; run;
data both; merge a(in=ina keep=id x) b(in=inb); by id; if ina and inb; run;
data only_a; merge a(in=p) b(in=q); by id; if p and not q; run;
