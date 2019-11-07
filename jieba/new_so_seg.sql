


CREATE EXTERNAL TABLE `news_no_seg`(
content string,
label string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '##@@##'
LINES TERMINATED BY '\n';

load data local inpath '/home/yuan/data/allfiles.txt' overwrite into table news_no_seg;




 create  table news_no_seg as
 select  regexp_replace(split(content,'##@@##')[0]," ","") as content,
 split(content,'##@@##')[1] as label from default.news;