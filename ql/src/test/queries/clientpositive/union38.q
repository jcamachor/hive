--create table test1 

create table test1( name String, id int, address String); 
insert into test1 values("Young", 1, "Sydney"), ("Jin", 2, "Mel"); 
analyze table test1 compute statistics for columns; 

create view test1_view as select * from test1; 
select * from test1_view; 


--create table test2 

create table test2( name String, id int, address String); 
insert into test2 values("Eun", 3, "Bri"), ("Kim", 4, "Ad"); 

create view test2_view as select * from test2; 
select * from test2_view; 

select * from test1 union select * from test2; 
select * from test1_view union select * from test2_view; 


--create view test_view using tables with union 

create view test_view as select * from test1 union select * from test2; 
select * from test_view; 


--create view test_view using tables with union all 

create view test_view1 as select * from test1 union all select * from test2; 
select * from test_view1; 


--create view test_view using temp table with union 

create view test_view2 as with tmp_1 as ( select * from test1 ), tmp_2 as (select * from test2 ) select * from tmp_1 union select * from tmp_2; 
select * from test_view2; 


--create view test_view using temp table with union all 

create view test_view3 as with tmp_1 as ( select * from test1 ), tmp_2 as (select * from test2 ) select * from tmp_1 union all select * from tmp_2; 
select * from test_view3; 


--create table test_table using temp table with union all 

create table test_table1 as with tmp_1 as ( select * from test1 ), tmp_2 as (select * from test2 ) select * from tmp_1 union all select * from tmp_2; 
select * from test_table1; 
