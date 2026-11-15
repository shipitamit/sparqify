data g; set s; by region; if first.region then n = 0; n + 1; run;
data lastrow; set s; by region; if last.region; run;
data lastout; set s; by region; if last.region then output; run;
data t; set s; by region; retain total 0; if first.region then total = 0; total + amt; run;
data u; set s; retain tot 100; tot = tot + amt; run;
data cf; set s; by id; retain last_v; if first.id then last_v = .; if not missing(v) then last_v = v; run;
data l; set s; prev = lag(x); d = dif(x); p2 = lag2(x); run;
