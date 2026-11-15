SET hive.exec.dynamic.partition=true;
INSERT OVERWRITE TABLE tgt PARTITION (dt) SELECT id, amt, dt FROM src WHERE amt > 0;
MSCK REPAIR TABLE tgt;
