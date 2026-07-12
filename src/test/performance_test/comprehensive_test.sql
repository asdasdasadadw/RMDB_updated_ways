set output_file off
create table t (a int, b int);
insert into t values (1, 5);
insert into t values (2, 8);
insert into t values (3, 12);
insert into t values (4, 6);
insert into t values (5, 20);
select a, b from t where a > 1 and b < 10;
EXPLAIN ANALYZE select a, b from t where a > 1 and b < 10;
create index t(a);
select a, b from t where a > 1 and b < 10;
select count(*), max(a), min(b), avg(b) from t;
select a, count(*), max(b) from t group by a having count(*) > 0 order by a;
select a, b from t order by b desc limit 3;
drop table t;
