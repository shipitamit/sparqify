libname mylib "/data";
%let thresh = 100;
proc import datafile="/data/sales.csv" out=sales dbms=csv replace; getnames=yes; run;

data work.sales2;
  set sales(where=(amount > 100) keep=id region amount qty);
  total = amount * qty;
  net = round(total * 0.8, 0.01);
  name = upcase(region) || '-' || strip(put(id, 5.));
  if total > 1000 then tier = 'gold';
  else if total > 100 then tier = 'silver';
  else tier = 'basic';
  if region = 'XX' then delete;
  drop qty;
run;

proc sort data=sales2 out=sorted nodupkey; by region descending total; run;

proc means data=sales2 noprint;
  class region tier;
  var total net;
  output out=summary mean=avg_total avg_net sum=sum_total sum_net;
run;

proc freq data=sales2; tables region*tier / out=freqs; run;

proc sql;
  create table top as
  select region, sum(total) as revenue from sales2 where total > 10 group by region order by revenue desc;
quit;

data x; set sales2; retain cum 0; cum + total; run;
proc print data=sorted(obs=10); run;
