CREATE TABLE IF NOT EXISTS 4sq_poi (v_id string, lat float, long float, cat_name string, country_code string) ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' ESCAPED BY '"' LINES TERMINATED BY '\n' STORED as TEXTFILE;

CREATE TABLE IF NOT EXISTS 4sq_cities (city_name string, city_lat float, city_long float, country_code string, country_name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' ESCAPED BY '"' LINES TERMINATED BY '\n' STORED as TEXTFILE;  

CREATE TABLE IF NOT EXISTS 4sq_checkpoint (u_id string, v_id string, date_time string, offset string) ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' ESCAPED BY '"' LINES TERMINATED BY '\n' STORED as TEXTFILE;


CREATE TABLE IF NOT EXISTS reddit(
	ranked SMALLINT,
	subr string,    
	created_utc string,    
	score int,    
	domain string,
        id string,    
	title string,    
	author string,    
	ups SMALLINT,    
	downs SMALLINT,    
	num_comments SMALLINT,    
	permalink string,    
	link_flair_text string,    
	over_18 string,    
	thumbnail string,    
	subreddit_id string,    
	editied string,    
	link_flair_css_class string,    
	author_flair_css_class string,    
	is_self string,    
	name string,    
	url  string,    
	distinguished string) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS Wiki(    prev string,     curr string,    type string,    n int) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties (    "separatorChar" = "\t",    "quoteChar"     = "'",    "escapeChar"    = "\\"   )    STORED AS TEXTFILE;
