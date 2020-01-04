-- This file will create an HBase table mapping titles to imdb ids
drop table if exists nprabhu_movie_titles_to_ids;
create external table nprabhu_movie_titles_to_ids (
  titleId string, 
  imdbId int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 
':key,
titlemap:imdbid')
TBLPROPERTIES ('hbase.table.name' = 'nprabhu_movie_titles_to_ids');

insert overwrite table nprabhu_movie_titles_to_ids
select 
concat_ws(":::", lower(title), cast(year as string), cast(imdbid as string)) as titleId, 
imdbId
from nprabhu_movies;
