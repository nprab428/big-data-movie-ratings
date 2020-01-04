-- This file will create an ORC table with movie metadata

drop table if exists nprabhu_movie_ratings_csv;
-- First, map the raw CSV data in Hive
create external table nprabhu_movie_ratings_csv(
  Tstamp bigint,
  UserId int,
  MovieId int,
  Rating float)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,"
)
STORED AS TEXTFILE
  location '/nprabhu/inputs/movie_ratings'
  tblproperties ("skip.header.line.count"="1");

-- Create an ORC table for movies metadata (Note "stored as ORC" at the end)
drop table if exists nprabhu_movie_ratings;
create table nprabhu_movie_ratings(
    UserId int,
    MovieId int,
    Rating float,
    Tstamp timestamp)
    stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table nprabhu_movie_ratings 
select 
    UserId, 
    MovieId,
    Rating,
    from_unixtime(cast(Tstamp as bigint)) as Tstamp
from nprabhu_movie_ratings_csv;
