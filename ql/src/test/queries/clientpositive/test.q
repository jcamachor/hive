set hive.mapred.mode=nonstrict;
set hive.optimize.index.filter=true;
set hive.optimize.aws.s3=true;

-- SORT_QUERY_RESULTS

drop table sales;
drop table things;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE sales (name STRING, id INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE TABLE things (name STRING, id INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

load data local inpath '../../data/files/sales.txt' INTO TABLE sales;
load data local inpath '../../data/files/sales.txt' INTO TABLE things;

EXPLAIN
SELECT sales.name,things.id FROM sales JOIN things ON (sales.id = things.id) WHERE things.name='James';

SELECT sales.name,things.id FROM sales JOIN things ON (sales.id = things.id) WHERE things.name='James';
