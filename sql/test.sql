create extension lsm3;

create table t(k bigint, val bigint);
create index lsm3_index on t using lsm3(k);
insert into t values (generate_series(1,100000), 1);
insert into t values (generate_series(1000001,200000), 2);
insert into t values (generate_series(2000001,300000), 3);
insert into t values (generate_series(1,100000), 1);
insert into t values (generate_series(1000001,200000), 2);
insert into t values (generate_series(2000001,300000), 3);
select * from t where k = 1;
select * from t where k = 1000000;
select * from t where k = 2000000;
select * from t where k = 3000000;
explain (COSTS OFF, TIMING OFF, SUMMARY OFF) select * from t where k = 1;
select lsm3_get_merge_count('lsm3_index') > 0;
drop table t;
