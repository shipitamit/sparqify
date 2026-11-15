%let thr = 100;
%macro flag(ds, lim=10);
  data &ds._f; set &ds; if amt > &lim then big = 1; else big = 0; run;
%mend flag;
%flag(sales, lim=&thr);
%flag(orders);
%macro rep(n);
  %do i = 1 %to &n; data t&i; set base; x = &i; run; %end;
%mend;
%rep(2)
%if &thr > 50 %then %do; data hi; set sales; where amt > &thr; run; %end;
%else %do; data lo; set sales; run; %end;
