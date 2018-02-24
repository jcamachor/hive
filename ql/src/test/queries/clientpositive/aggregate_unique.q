-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.fetch.column.stats=true;

create table emps (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');
insert into emps values (100, 10, 'Bill', 10000, 1000), (200, 20, 'Eric', 8000, 500),
  (150, 10, 'Sebastian', 7000, null), (110, 10, 'Theodore', 10000, 250), (110, 10, 'Bill', 10000, 250);
analyze table emps compute statistics for columns;

alter table emps add constraint pk1 primary key (empid, deptno) disable novalidate rely;

explain
select count(1) from (
  select empid, deptno, name, salary, commission
  from emps as inner_tab
  group by empid, deptno, name, salary, commission) outer_tab;

explain
select count(1) from (
  select count(1), empid, deptno, name, salary, commission
  from emps as inner_tab
  group by empid, deptno, name, salary, commission) outer_tab;

explain
select count(1) from (
  select count(1), empid, name, salary, commission
  from emps as inner_tab
  group by empid, name, salary, commission) outer_tab;
