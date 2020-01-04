-- This file will create an ORC table with movie metadata

drop table if exists nprabhu_movies_metadata_csv;
-- First, map the raw CSV data in Hive
create external table nprabhu_movies_metadata_csv(
  Adult boolean,
  BelongsToCollection string,
  Budget int,
  Genres string,
  Homepage string,
  Id int,
  ImdbId string,
  OriginalLanguage string,
  OriginalTitle string,
  Overview string,
  Popularity float,
  PosterPath string,
  ProductionCompanies string,
  ProductionCountries string,
  ReleaseDate date,
  Revenue bigint, 
  Runtime smallint,
  SpokenLanguages string,
  Status string,
  Tagline string,
  Title string,
  Video boolean,
  VoteAverage float,
  VoteCount smallint)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/nprabhu/inputs/movies_metadata'
  tblproperties ("skip.header.line.count"="1");

-- Run a test query to make sure the above worked correctly
-- select ImdbId,OriginalTitle from nprabhu_movies_metadata_csv limit 5;

-- Create an ORC table for movies metadata (Note "stored as ORC" at the end)
drop table if exists nprabhu_movies_metadata;
create table nprabhu_movies_metadata(
    Adult boolean,
    BelongsToCollection string,
    Budget int,
    Genres string,
    Homepage string,
    Id int,
    ImdbId string,
    OriginalLanguage string,
    OriginalTitle string,
    Overview string,
    Popularity float,
    PosterPath string,
    ProductionCompanies string,
    ProductionCountries string,
    ReleaseDate date,
    Revenue bigint, 
    Runtime smallint,
    SpokenLanguages string,
    Status string,
    Tagline string,
    Title string,
    Video boolean,
    VoteAverage float,
    VoteCount smallint)
    stored as orc;

-- Copy the CSV table to the ORC table and filter out incomplete data
insert overwrite table nprabhu_movies_metadata 
select * from nprabhu_movies_metadata_csv
where Runtime>0;
