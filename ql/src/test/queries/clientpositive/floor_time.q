drop table floor_udf;

create table floor_udf (t timestamp);
from (select * from src tablesample (1 rows)) s
  insert overwrite table floor_udf
    select '2011-05-06 07:08:09.1234567';

select t
from floor_udf;

explain
select floor_day(t)
from floor_udf;

select floor_day(t)
from floor_udf;

-- new syntax
explain
select floor(t to day)
from floor_udf;

select floor(t to day)
from floor_udf;


select floor(t to second)
from floor_udf;

select floor(t to minute)
from floor_udf;

select floor(t to hour)
from floor_udf;

select floor(t to week)
from floor_udf;

select floor(t to month)
from floor_udf;

select floor(t to quarter)
from floor_udf;

select floor(t to year)
from floor_udf;
