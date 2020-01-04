-- This file will create an ORC table with movie metadata
drop table if exists nprabhu_movie_genres_csv;
-- First, map the raw CSV data in Hive
create external table nprabhu_movie_genres_csv(
  MovieId int,
  Title string,
  Year smallint,
  Romance boolean,
  Crime boolean,
  Animation boolean,
  FilmNoir boolean,
  SciFi boolean,
  Action boolean,
  War boolean,
  Western boolean,
  Children boolean,
  Documentary boolean,
  Comedy boolean,
  Thriller boolean,
  Drama boolean,
  Horror boolean,
  Musical boolean,
  Mystery boolean,
  IMAX boolean,
  Adventure boolean,
  Fantasy boolean)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,"
)
STORED AS TEXTFILE
  location '/nprabhu/inputs/movie_genres'
  tblproperties ("skip.header.line.count"="1");

-- Create an ORC table for movie genres data (Note "stored as ORC" at the end)
drop table if exists nprabhu_movie_genres_hive;
create table nprabhu_movie_genres_hive(
  MovieId int,
  Title string,
  Year smallint,
  Romance boolean,
  Crime boolean,
  Animation boolean,
  FilmNoir boolean,
  SciFi boolean,
  Action boolean,
  War boolean,
  Western boolean,
  Children boolean,
  Documentary boolean,
  Comedy boolean,
  Thriller boolean,
  Drama boolean,
  Horror boolean,
  Musical boolean,
  Mystery boolean,
  IMAX boolean,
  Adventure boolean,
  Fantasy boolean)
  stored as orc;

-- Copy the CSV table to the ORC table and filter out incomplete data
insert overwrite table nprabhu_movie_genres_hive 
select * from nprabhu_movie_genres_csv;
