
-- Try and practice loading student table for basic understanding before loading Yelp dataset
CREATE TABLE IF NOT EXISTS students (
name STRING ,
id INT ,
subjects ARRAY < STRING >,
feeDetails MAP < STRING , FLOAT >,
phoneNumber STRUCT <areacode: INT , number : INT > )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '#'
MAP KEYS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Syntax:
-- load data inpath 'add path to your file here' overwrite into table <table_Name>;

-- Query:
load data '/home/hadoop/student.dat' overwrite into table students;

-- To check the output

Select * FROM students;
Select explode(feedetails) FROM students;

-- 2. Upper() : -- Converts the column values into Uppercase 
Select upper ( name ) from students;

-- 3. Regex_Replace() -- Name is converted in Uppercase,concatenate name and ID column values , To remove mid space using reg exp replace with blank 

Select regexp_replace(concat(upper(name),id ),' ','') as username from
students;

-- General Points to note:

-- count(*) - returns the total no. of retrieved rows, including rows containing null values
-- count(column) - returns the no. of rows for which the given column is not null
-- sum(column) - returns the sum of values in the given column
-- avg(column) - returns the average of values in the given column (= sum (column) /count(column))

-- =========== Loading YELP Dataset ===========================================================================

-- Add Jar do read Json file and load data into table.

add jar s3://elasticmapreduce/samples/hive-ads/libs/jsonserde.jar;

-- Table to load JSON files - Users.json 
CREATE TABLE yelp_user_hdfs (
user_id string,
name string,
review_count bigint,
yelping_since string,
friends array<string>,
useful_rating double,
Funny_rating double,
Cool_rating double,
Fans_rating double,
elite array<string>,
average_stars float
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
with serdeproperties ( 'paths' = '' );

-- Loading data into table created above using datafile received from Yelp.
-- local keyword is used when logged in as hadoop in HDFS,
-- if logged as ec2-user using an instance in HDFS, then copy the data file to root user directories
-- use sudo -i to move to root user  

load data local inpath '/home/hadoop/users_250.json' overwrite into table yelp_user_hdfs;

-- Table to load JSON files - review.json 
CREATE TABLE yelp_review_hdfs (
review_id string, 
user_id string,
business_id string,
stars bigint,
r_date string,
text string,
useful double,
funny double,
 cool double
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
with serdeproperties ( 'paths' = '' );

load data local inpath '/home/hadoop/review_250.json' overwrite into table yelp_review_hdfs;

-- 1. Number of reviews
select count (*) as review_count
from yelp_user_hdfs;

- 2. check the size of review text
select text,size(split(text,' ')) as words_count
from yelp_review_hdfs limit 10 ;

-- 3. Gives the total number of records, total number of unique user, min and avg star values

select count (*) as Total_recs_count, count (distinct user_id) as
user_count,
min (average_stars) as min_star,
avg (average_stars) as avg_star
from yelp_user_hdfs;

-- What is the avg number of review each reviwer given

select avg (review_cnt)
from (
select user_id, count (*) as review_cnt
from yelp_user_hdfs
group by user_id
)a;

-- 2. Average reviews count by each user

select avg (stars_count)
from (
select review_id, sum (stars) as stars_count
from yelp_review_hdfs
group by review_id
)a;

-- JOIN between User and review table

select yelp_user_hdfs.user_id,yelp_review_hdfs.review_id
from yelp_user_hdfs JOIN yelp_review_hdfs 
ON yelp_user_hdfs.user_id=yelp_review_hdfs.user_id limit 10;



-- YELP partition
-- Creating a year column using substr function, will be used as partition key.

CREATE TABLE yelp_user_year (
user_id string,
name string,
review_count bigint,
yelping_since string,
friends array<string>,
useful_rating double,
Funny_rating double,
Cool_rating double,
Fans_rating double,
elite array<string>,
average_stars float,
yr string
);

-- Loading data with year column value
insert overwrite table yelp_user_year 
select user_id,name,review_count,yelping_since,friends,useful_rating,funny_rating,cool_rating,fans_rating,elite,average_stars,substr(yelping_since,1,4)  from yelp_user_hdfs;

select distinct(yr) from yelp_user_year;
select min(yr), max(yr) from yelp_user_year;

-- Set partition property , by default set to strict
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode = nonstrict;

-- Creating partition table partition by year.
create external table if not exists
yelp_user_hdfs_partioned (
user_id string,
name string,
review_count bigint,
yelping_since string,
useful_rating double,
Funny_rating double,
Cool_rating double,
Fans_rating double,
average_stars float
)
partitioned by
(yr string);

-- loading data
insert overwrite table yelp_user_hdfs_partioned partition (yr)
select user_id,
name, 
review_count, 
yelping_since, 
useful_rating, 
Funny_rating, 
Cool_rating, 
Fans_rating ,
average_stars,
yr
from yelp_user_year;

-- Displaying partitions created  
show partitions yelp_user_hdfs_partioned;

-- Retrival Time Comparison between partitioned vs non-partitioned tables.

select count(*) as user_count
from yelp_user_hdfs_partioned;

select count(*) as user_count
from yelp_user_hdfs;

-- YELP BUCKETING
-- Buckets created using clustered by keyword. in this case we are creating 4 buckets.

create external table if not exists
yelp_user_hdfs_partioned_yr_bucket (
user_id string,
name string,
review_count bigint,
yelping_since string,
useful_rating double,
Funny_rating double,
Cool_rating double,
Fans_rating double,
average_stars float
)
partitioned by (yr string)
clustered by (user_id) into 4 buckets;

-- Loading data into table 
insert overwrite table yelp_user_hdfs_partioned_yr_bucket partition (yr)
select user_id,
name, 
review_count, 
yelping_since, 
useful_rating, 
Funny_rating, 
Cool_rating, 
Fans_rating ,
average_stars,
yr
from yelp_user_hdfs_partioned;

-- Creating ORC table 


CREATE EXTERNAL TABLE yelp_user_hdfs_ORC (
user_id string,
name string,
review_count bigint,
yelping_since string,
friends array<string>,
useful_rating double,
Funny_rating double,
Cool_rating double,
Fans_rating double,
average_stars float
)
partitioned by (yr string)
clustered by (user_id) into 4 buckets;
STORED AS ORC 
LOCATION 's3//yelphivedemo01/users_orc'
TBLPROPERTIES ('orc.compress'='SNAPPY');

insert overwrite table yelp_user_hdfs_ORC partition(yr)
select 
user_id,
name,
review_count,
yelping_since,
friends,
useful_rating,
Funny_rating,
Cool_rating,
Fans_rating,
average_stars,
yr
from yelp_user_year;

select * from yelp_user_hdfs_ORC;

-- Try practicing the same steps for review dataset.
-- After creating review table, try the below udf

select * from yelp_review_hdfs_partioned where yr='2017'

-- check the size of review text
select text,size(split(text,' ')) as words_count
from yelp_review_hdfs_partioned where text is not null limit 10 ;

-- sentences UDF 
select explode(sentences(lower(text))) as words_length
from yelp_review_hdfs_partioned where text is not null limit 10 ;
 
-- N Grams of length 2 , list top 6 words of length 2
select explode(ngrams(sentences(lower(text)),2,6)) as words_length_of_2
from yelp_review_hdfs_partioned;.

