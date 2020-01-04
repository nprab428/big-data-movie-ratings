-- This file will create an ORC table with movie links

drop table if exists nprabhu_movie_links_csv;
-- First, map the raw CSV data in Hive
create external table nprabhu_movie_links_csv(
  MovieId int,
  ImdbId int,
  TmdbId int)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,"
)
STORED AS TEXTFILE
  location '/nprabhu/inputs/movie_links'
  tblproperties ("skip.header.line.count"="1");

-- Create an ORC table for movie links data (Note "stored as ORC" at the end)
drop table if exists nprabhu_movie_links_hive;
create table nprabhu_movie_links_hive(
    MovieId int,
    ImdbId int,
    TmdbId int)
    stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table nprabhu_movie_links_hive 
select * from nprabhu_movie_links_csv;

-- Create mapping table in hbase used for streaming-layer
drop table if exists nprabhu_movie_links_hbase;
create external table nprabhu_movie_links_hbase(
    MovieId int,
    ImdbId int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 
':key,
links:imdbid'
)
TBLPROPERTIES ('hbase.table.name' = 'nprabhu_movie_links_hbase'); 

-- Populate hbase table from link table in hive
insert overwrite table nprabhu_movie_links_hbase 
select MovieId, ImdbId from nprabhu_movie_links_hive;