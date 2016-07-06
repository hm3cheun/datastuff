create table if not exists 4sq_poi_us as select * from 4sq_poi where country_code =="US";

create table if not exists 4sq_cities_us as select * from 4sq_cities where country_code =="US";  

create table if not exists poi_us_join_checkpoint as select  t1.v_id as v_id,  t2.cat_name as category, t2.lat as lat, t2.long as long, t2.country_code as country_code, t1.u_id as u_id, t1.date_time as date_time from 4sq_checkpoint t1, 4sq_poi_us t2 where t1.v_id == t2.v_id;  

create table if not exists 4sq_us as select  t1.v_id as v_vid, t1.country_code as country_code, t1.category as category, t1.lat as lat, t1.long as long, t1.u_id as u_id, t1.date_time as date_time, t2.city_name as city_name, t2.city_long as city_long, t2.city_lat as city_lat, t2.country_name as country_name from  poi_us_join_checkpoint t1, 4sq_cities_us t2  where t1.country_code == t2.country_code;  


create table if not exists 4sq_us_day as select  v_vid, country_code, substring(date_time,9,2) as dayofmonth, category, lat, long, u_id, date_time, city_name, city_long, city_lat, country_name from 4sq_us;  

create table if not exists reddit_april_day as select     ranked,     subr,    created_utc,    substring(created_utc,9,2) as dayofmonth,    substring(created_utc,6,2) as month,    score,    domain,    id,    title,    author,    ups,    downs,    num_comments,    permalink,    link_flair_text, over_18,    thumbnail,    subreddit_id,    editied,    link_flair_css_class,    author_flair_css_class,    is_self,    name,    url,    distinguished from reddit;  

create table temp_4sq_us_day as SELECT * FROM 4sq_us_day t1 limit 100000;  create table temp_reddit_april_day as select * from reddit_april_day limit 10000;  select * from temp_4sq_us_day t1, temp_reddit_april_day t2 where t1.dayofmonth == t2.dayofmonth;
