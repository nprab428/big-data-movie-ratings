-- This file will create Hive and HBase tables with ratings count per movie
drop table if exists nprabhu_movie_ratings_count_hive;
create external table nprabhu_movie_ratings_count_hive (
  imdbId int, 
  title string,
  year smallint,
  total bigint,
  half bigint,
  one bigint,
  one_and_a_half bigint,
  two bigint,
  two_and_a_half bigint,
  three bigint,
  three_and_a_half bigint,
  four bigint,
  four_and_a_half bigint,
  five bigint) stored as orc;

drop table if exists nprabhu_movie_ratings_count_hbase;
create external table nprabhu_movie_ratings_count_hbase (
  imdbId int, 
  title string,
  year smallint,
  total bigint,
  half bigint,
  one bigint,
  one_and_a_half bigint,
  two bigint,
  two_and_a_half bigint,
  three bigint,
  three_and_a_half bigint,
  four bigint,
  four_and_a_half bigint,
  five bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 
':key,
ratings:title,
ratings:year,
ratings:total#b,
ratings:half#b,
ratings:one#b,
ratings:one_and_a_half#b,
ratings:two#b,
ratings:two_and_a_half#b,
ratings:three#b,
ratings:three_and_a_half#b,
ratings:four#b,
ratings:four_and_a_half#b,
ratings:five#b')
TBLPROPERTIES ('hbase.table.name' = 'nprabhu_movie_ratings_count_hbase');

insert overwrite table nprabhu_movie_ratings_count_hive
select 
m.imdbId, 
m.title,
m.year,
count(1),
count(if(r.rating==0.5, 1, null)),
count(if(r.rating==1,  1, null)),
count(if(r.rating==1.5, 1, null)),
count(if(r.rating==2, 1, null)),
count(if(r.rating==2.5, 1, null)),
count(if(r.rating==3, 1, null)),
count(if(r.rating==3.5, 1, null)),
count(if(r.rating==4, 1, null)),
count(if(r.rating==4.5, 1, null)),
count(if(r.rating==5, 1, null))
from nprabhu_movies m
join nprabhu_movie_links_hive l join nprabhu_movie_ratings r
on l.imdbid = m.imdbid
and r.movieid = l.movieid
group by m.imdbId, m.title, m.year;

insert overwrite table nprabhu_movie_ratings_count_hbase
select
imdbId,
title,
year,
total,
half,
one,
one_and_a_half,
two,
two_and_a_half,
three,
three_and_a_half,
four,
four_and_a_half,
five
from nprabhu_movie_ratings_count_hive;