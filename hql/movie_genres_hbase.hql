-- This file will create a hbase table mapping imdbid to all its genres

-- Hbase table to store genre-average rankings (in sorted order)
drop table if exists nprabhu_movie_genres_hbase;
create external table nprabhu_movie_genres_hbase (
  imdbId string, 
  genres string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 
':key,
genres:genres')
TBLPROPERTIES ('hbase.table.name' = 'nprabhu_movie_genres_hbase');

insert overwrite table nprabhu_movie_genres_hbase
select 
    imdbId, 
    concat(
        if(action,'ACTION:::',''),
        if(adventure,'ADVENTURE:::',''),
        if(animation,'ANIMATION:::',''),
        if(children,'CHILDREN:::',''),
        if(comedy,'COMEDY:::',''),
        if(crime,'CRIME:::',''),
        if(documentary,'DOCUMENTARY:::',''),
        if(drama,'DRAMA:::',''),
        if(fantasy,'FANTASY:::',''),
        if(filmnoir,'FILMNOIR:::',''),
        if(horror,'HORROR:::',''),
        if(imax,'IMAX:::',''),
        if(musical,'MUSICAL:::',''),
        if(mystery,'MYSTERY:::',''),
        if(romance,'ROMANCE:::',''),
        if(scifi,'SCIFI:::',''),
        if(thriller,'THRILLER:::',''),
        if(war,'WAR:::',''),
        if(western,'WESTERN:::','')
    ) as genres
from nprabhu_movies;
