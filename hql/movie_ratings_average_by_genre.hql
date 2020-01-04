-- This file will create a hbase table with avg ratings per movie & genre
-- Hbase table to store genre-average rankings (in sorted order)
drop table if exists nprabhu_movie_ratings_average_by_genre_hbase;
create external table nprabhu_movie_ratings_average_by_genre_hbase (
  genreRatingId string, 
  avgRating float,
  imdbId int,
  title string,
  year smallint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 
':key,
ratings:avgRating,
ratings:imdbid,
ratings:title,
ratings:year')
TBLPROPERTIES ('hbase.table.name' = 'nprabhu_movie_ratings_average_by_genre_hbase');

-- Complete set of genres
--  'All'
--  'Action',
--  'Adventure',
--  'Animation',
--  'Children',
--  'Comedy',
--  'Crime',
--  'Documentary',
--  'Drama',
--  'Fantasy',
--  'Film-Noir',
--  'Horror',
--  'IMAX',
--  'Musical',
--  'Mystery',
--  'Romance',
--  'Sci-Fi',
--  'Thriller',
--  'War',
--  'Western'
insert overwrite table nprabhu_movie_ratings_average_by_genre_hbase
select 
    concat_ws(":::", "ALL", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId

union
select 
    concat_ws(":::", "ACTION", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.action==true

union
select 
    concat_ws(":::", "ADVENTURE", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.adventure==true

union
select 
    concat_ws(":::", "ANIMATION", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.animation==true

union
select 
    concat_ws(":::", "CHILDREN", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.children==true

union
select 
    concat_ws(":::", "COMEDY", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.comedy==true

union
select 
    concat_ws(":::", "CRIME", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.crime==true

union
select 
    concat_ws(":::", "DOCUMENTARY", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.documentary==true

union
select 
    concat_ws(":::", "DRAMA", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.drama==true

union
select 
    concat_ws(":::", "FANTASY", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.fantasy==true

union
select 
    concat_ws(":::", "FILMNOIR", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.filmnoir==true

union
select 
    concat_ws(":::", "HORROR", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.horror==true

union
select 
    concat_ws(":::", "IMAX", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.imax==true

union
select 
    concat_ws(":::", "MUSICAL", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.musical==true

union
select 
    concat_ws(":::", "MYSTERY", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.mystery==true

union
select 
    concat_ws(":::", "ROMANCE", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.romance==true

union
select 
    concat_ws(":::", "SCIFI", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.scifi==true

union
select 
    concat_ws(":::", "THRILLER", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.thriller==true

union
select 
    concat_ws(":::", "WAR", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.war==true

union
select 
    concat_ws(":::", "WESTERN", cast(mra.avgRating as string), cast(m.imdbId as string)) as genreRatingId, 
    mra.avgRating,
    m.imdbId,
    m.title,
    m.year
from nprabhu_movie_ratings_average_hive mra
join nprabhu_movies m
on m.imdbId = mra.imdbId
where m.western==true;
