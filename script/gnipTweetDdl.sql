Preprocess with pig 
rmf '/user/gpadmin/twitter/csv'
a = load '/user/gpadmin/twitter/data' USING TextLoader AS (line:chararray);
b = foreach a generate REPLACE(line,'"location"','"locations"') as line;
c = store b into '/user/gpadmin/twitter/csv';

Process with hive
create table with json SerDe
create table tweets_cached row format delimited fields terminated by '|'  stored as textfile as select id,regexp_replace(body,'\n','') from default.tweets ;
 

CREATE EXTERNAL TABLE tweets (
id string,
objectType string,
verb string,
actor STRUCT<
 id:STRING,
 link:STRING,
 displayName:STRING,
 postedTime:STRING,
 image:STRING,
 summary:STRING,
 friendsCount:INT,
 followersCount:INT,
 listedCount:INT,
 statusesCount:INT,
 twitterTimeZone:STRING,
 verified:boolean,
 utcOffset:STRING,
 preferredUsername:STRING>, 
generator STRUCT<displayName:STRING,link:STRING>,
twitter_entities STRUCT<
      urls:ARRAY<STRUCT<expanded_url:STRING>>,
      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
      hashtags:ARRAY<STRUCT<text:STRING>>>,
postedTime string,
link string,
body string,
favoritesCount int,
twitter_filter_level string,
twitter_lang string,
retweetCount int)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/gpadmin/twitter/data';

ALTER TABLE tweetsream SET SERDEPROPERTIES ( "ignore.malformed.json" = "true");

--test

CREATE EXTERNAL TABLE tweets (
id string,objectType string,
verb string,
postedTime string,
link string,
body string,
favoritesCount int,
twitter_filter_level string,
twitter_lang string,
retweetCount int)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe';


--test 2
CREATE EXTERNAL TABLE tweets_raw (
id string,
objectType string,
actor STRUCT<
 objectType:STRING,
 id:STRING,
 link:STRING,
 displayName:STRING,
 postedTime:STRING,
 image:STRING,
 summary:STRING,
 friendsCount:INT,
 followersCount:INT,
 listedCount:INT,
 statusesCount:INT,
 twitterTimeZone:STRING,
 verified:boolean,
 utcOffset:STRING,
 preferredUsername:STRING,
 `location`:STRUCT<objectType:STRING,displayName:STRING>>, 
verb string,
postedTime string,
generator STRUCT<displayName:STRING,link:STRING>,
provider STRUCT<objectType:STRING,displayName:STRING,link:STRING>,
link string,
body string,
favoritesCount int,
twitter_entities STRUCT<
      hashtags:ARRAY<STRUCT<text:STRING>>,
      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>>,
twitter_filter_level string,
twitter_lang string,
retweetCount int,
gnip STRUCT<
  profileLocations:ARRAY<STRUCT<
    objectType:STRING,
    geo:STRUCT<
	   type:STRING,
	   ARRAY<0:STRING,1:STRING>>,
	address: <
	   country:STRING,
	   countryCode:STRING,
	   locality:STRING,
	   region:LAMPUNG>,
	displayName:STRING>,  
  >
  )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/gpadmin/twitter/data';

ALTER TABLE tweets_raw SET SERDEPROPERTIES ( "ignore.malformed.json" = "true");

=========================================================================================================
--FULL COLUMN TWEET GNIP
DROP TABLE tweets_raw;
CREATE EXTERNAL TABLE tweets_raw (
id string,
objectType string,
actor STRUCT<
 objectType:STRING,
 id:STRING,
 link:STRING,
 displayName:STRING,
 postedTime:STRING,
 image:STRING,
 summary:STRING,
 friendsCount:INT,
 followersCount:INT,
 listedCount:INT,
 statusesCount:INT,
 twitterTimeZone:STRING,
 verified:boolean,
 utcOffset:STRING,
 preferredUsername:STRING,
 `location`:STRUCT<objectType:STRING,displayName:STRING>>, 
verb string,
postedTime string,
generator STRUCT<displayName:STRING,link:STRING>,
provider STRUCT<objectType:STRING,displayName:STRING,link:STRING>,
link string,
body string,
favoritesCount int,
twitter_entities STRUCT<
      hashtags:ARRAY<STRUCT<text:STRING>>,
      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>>,
twitter_filter_level string,
twitter_lang string,
retweetCount int,
gnip STRUCT<
  profileLocations:ARRAY<STRUCT<
    objectType:STRING,
    geo:STRUCT<type:STRING,coordinates:ARRAY<STRING>>,
    address:STRUCT<country:STRING,countryCode:STRING,locality:STRING,region:STRING>,
    displayName:STRING>>>
  )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/gpadmin/twitter/data';


ALTER TABLE tweets_raw SET SERDEPROPERTIES ( "ignore.malformed.json" = "true");
=========================================================================================================

--in memory
create table tweetstream_cached 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' as
SELECT 
 id,objectType,verb,actor.id as actor_id,
 actor.link as actor_link ,actor.postedTime as actor_postedTime,actor.image as actor_image,
 actor.friendsCount as actor_friendsCount,actor.followersCount as actor_followersCount,actor.listedCount as actor_listedCount,
 actor.statusesCount as actor_statusesCount,actor.twitterTimeZone as actor_twitterTimeZone,actor.verified as actor_verified,actor.utcOffset as actor_utcOffset,
 actor.preferredUsername as actor_preferredUsername,generator.link as generator_link,
 twitter_entities.urls.expanded_url as twitter_entities_urls_expanded_url,twitter_entities.user_mentions.screen_name as twitter_entities_user_mentions_screen_name,twitter_entities.hashtags.text as twitter_entities_hashtags_text,
 postedTime,link,favoritesCount,twitter_filter_level,twitter_lang,retweetCount
FROM default.tweetsream limit 28431; --exclude body & actor.summary as actor_summary & actor.displayName as actor_displayName & generator.displayName as generator_displayName, twitter_entities.user_mentions.name as twitter_entities_user_mentions_name

--------------------------------------

DROP TABLE IF EXISTS tweets_cached; 
create table tweets_cached ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n' STORED AS TEXTFILE AS 
select 
id ,
objectType ,
actor.objectType as actor_objectType,			
actor.id as actor_id,
actor.link as actor_link,
regexp_replace(regexp_replace(actor.displayName,'\n',''),'\r','') as actor_displayName,
actor.postedTime as actor_postedTime,
actor.image as actor_image,
regexp_replace(regexp_replace(actor.summary,'\n',''),'\r','') as actor_summary,
actor.friendsCount as actor_friendsCount,
actor.followersCount as actor_followersCount,
actor.listedCount as actor_listedCount,
actor.statusesCount as actor_statusesCount ,
actor.twitterTimeZone as actor_twitterTimeZone,
actor.verified as actor_verified,
actor.utcOffset as actor_utcOffset,
actor.preferredUsername as actor_preferredUsername,
actor.`location`.objectType as  actor_location_objectType,
regexp_replace(regexp_replace(actor.`location`.displayName,'\n',''),'\r','') as actor_location_displayName,
verb ,
postedTime ,
regexp_replace(regexp_replace(generator.displayName,'\n',''),'\r','')  as generator_displayName,
generator.link as generator_link,
provider.objectType as provider_objectType ,
provider.displayName as provider_displayName,
provider.link as provider_link,
link ,
regexp_replace(regexp_replace(body,'\n',''),'\r','') as body ,
favoritesCount  ,
twitter_entities.hashtags.text as twitter_entities_hashtags_text ,
twitter_entities.user_mentions.screen_name as twitter_entities_user_mentions_screen_name,
twitter_filter_level ,
twitter_lang ,
retweetCount 
from tweets_raw;

--location:STRUCT<objectType:STRING,displayName:STRING>>,


--Trending Hastags

select twitter_entities.hashtags.text[1] as hastags,count(1) as cnt  from (select * from tweetsream limit 20000) tweetsream where twitter_entities.hashtags.text[1] is not null group by twitter_entities.hashtags.text[1]  order by cnt desc limit 5 ;

select twitter_entities.hashtags.text[1] as hastags,count(1) as cnt  from  
tweets_raw where twitter_entities.hashtags.text[1] is not null 
group by twitter_entities.hashtags.text[1]  order by cnt desc;


--Trending Word 
select word, count(*) from t_tweetsream  LATERAL VIEW explode(split(body,' ')) ltable as word group by word;

 select actor.preferredUsername,  word from t_tweetsream  LATERAL VIEW explode(split(body,' ')) ltable as word ;

 
--Category grouping 

 select category, count(1) as cnt  from (select a.username, b.category from t_twittWord_cached a join word_cat_cached b on lower(a.word) = lower(b.word)) tbl group by category ;
 
 