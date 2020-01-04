-- This file will create a hive and hbase tables with avg ratings per movie
drop table if exists nprabhu_movie_ratings_average_hive;
create table nprabhu_movie_ratings_average_hive (
  imdbId int, 
  title string,
  year smallint,
  avgRating float,
  total int)
  stored as orc;

-- Hbase table to serve as secondary index for nprabhu_movie_ratings_average_by_genre_hbase
drop table if exists nprabhu_movie_ratings_average_secondary_index;
create external table nprabhu_movie_ratings_average_secondary_index (
  imdbId int,
  avgRating float)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 
':key,
ratings:avgRating'
)
TBLPROPERTIES ('hbase.table.name' = 'nprabhu_movie_ratings_average_secondary_index');

-- Populate tables
insert overwrite table nprabhu_movie_ratings_average_hive
select 
    imdbId, 
    title,
    year,
    (0.5*half + 1*one + 1.5*one_and_a_half + 
    2*two + 2.5*two_and_a_half + 3*three + 
    3.5*three_and_a_half + 4*four + 
    4.5*four_and_a_half + 5*five)/total as avgRating,
    total
from nprabhu_movie_ratings_count_hive
where total>=100; -- fix a minimum no. of ratings to qualify for listing

insert overwrite table nprabhu_movie_ratings_average_secondary_index
select 
    imdbId,
    avgRating
from nprabhu_movie_ratings_average_hive;