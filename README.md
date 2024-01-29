<h1> <img src='https://www.woodmark.de/files/theme/layout/images/logo25years.svg' width=80> 
Zero to Snowflake  Scripts ❄️ 
</h1>



## Preparing to Load Data  

Creating the database.
```SQL
USE ROLE SYSADMIN ; 
CREATE DATABASE CITIBIKE 
```
Creating the trips table
```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE COMPUTE_WH;

create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);
```
Creating the citibike_trips external stage 
```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE COMPUTE_WH;

CREATE STAGE citibike_trips 
URL = 's3://snowflake-workshop-lab/citibike-trips-csv/' 
DIRECTORY = ( ENABLE = true );

list @citibike_trips;
```
Creating the csv file format 
```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE COMPUTE_WH;

create or replace file format csv type='csv'
compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

show file formats in database citibike;
```

## Loading Data

Change compute_wh warehouse size 
```SQL
alter warehouse compute_wh set warehouse_size='small';
```

Load the data from the stage
```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE COMPUTE_WH;

copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

```

Clean the table
```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE COMPUTE_WH;

truncate table trips;

--verify table is clear
select * from trips limit 10;

```

Change warehouse size 
```SQL
--change warehouse size from small to large (4x)
alter warehouse compute_wh set warehouse_size='large';

--load data with large warehouse
show warehouses;

```

Load the data from the stage
```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE COMPUTE_WH;

copy into trips from @citibike_trips
file_format=CSV;

```

Create ANALYTICS_WH
```SQL
CREATE OR REPLACE WAREHOUSE ANALYTICS_WH WAREHOUSE_SIZE=LARGE MAX_CLUSTER_COUNT =1;  

```


## Working with Queries, the Results Cache, & Cloning

Execute Some Queries
```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE ANALYTICS_WH ;

select * from trips limit 20;

```

```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE ANALYTICS_WH ;

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

```

```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE ANALYTICS_WH ;

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

```

```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE ANALYTICS_WH ;

select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

```
Clone a Table

```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE ANALYTICS_WH ;

create table trips_dev clone trips;

```

## Working with Semi-Structured Data, Views, & Joins
Create a New Database and Table for the Data

```SQL
create database weather;

use role sysadmin;

use warehouse compute_wh;

use database weather;

use schema public;

create table json_weather_data (v variant);

```

Create Another External Stage

```SQL
use database weather;

use warehouse compute_wh;

create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

list @nyc_weather;

```

Load and Verify the Semi-structured Data

```SQL
use database weather;

use warehouse compute_wh;

copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);

select * from json_weather_data limit 10;

```

Create a View and Query Semi-Structured Data

```SQL
use database weather;

use warehouse compute_wh;

create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';


select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

```

Use a Join Operation to Correlate Against Data Sets

```SQL
use database weather;

use warehouse compute_wh;

select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;


```

## Using Time Travel
Drop and Undrop a Table

```SQL
use database weather;

use warehouse compute_wh;

drop table json_weather_data;

select * from json_weather_data limit 10;

```

```SQL
use database weather;

use warehouse compute_wh;

undrop table json_weather_data;

select * from json_weather_data limit 10;

```
Roll Back a Table

```SQL
use role sysadmin;

use warehouse compute_wh;

use database citibike;

use schema public;

update trips set start_station_name = 'oops';

```


```SQL
use warehouse compute_wh;

use database citibike;

select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

```


```SQL
use warehouse compute_wh;

use database citibike;

set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time desc limit 1);

```


```SQL
use warehouse compute_wh;

use database citibike;

create or replace table trips as
(select * from trips before (statement => $query_id));

```

```SQL
use warehouse compute_wh;

use database citibike;

select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

```


## Working with Roles, Account Admin, & Account Usage
Create a New Role and Add a User

```SQL
use role accountadmin;

create role junior_dba;

grant role junior_dba to user YOUR_USERNAME_GOES_HERE;

```

check new role  privileges 
```SQL
use role junior_dba;

```
add privileges to  junior_dba role 

```SQL
use role accountadmin;

grant usage on warehouse compute_wh to role junior_dba;

grant usage on database citibike to role junior_dba;

grant usage on database weather to role junior_dba;

```
verify new privileges
```SQL
use  role junior_dba;

use warehouse compute_wh;
```

## Resetting Your Snowflake Environment
```SQL
USE DATABASE CITIBIKE ; 

USE WAREHOUSE ANALYTICS_WH ;

create table trips_dev clone trips;

```

## Resetting Your Snowflake Environment
```SQL
use role accountadmin;

drop share if exists zero_to_snowflake_shared_data;
-- If necessary, replace "zero_to_snowflake-shared_data" with the name you used for the share

drop database if exists citibike;

drop database if exists weather;

drop warehouse if exists analytics_wh;

drop role if exists junior_dba;

```
---
Powered with 💚 by [WOODMARK](https://www.woodmark.de/en/)
