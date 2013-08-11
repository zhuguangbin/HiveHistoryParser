CREATE TABLE IF NOT EXISTS hive_session_log(
sessionid STRING,
hostip STRING,
username STRING,
starttime BIGINT,
endtime   BIGINT
)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE;

load data local inpath '/usr/local/hadoop/hivehistoryparser/out/sessionLog'  OVERWRITE INTO TABLE  hive_session_log;

CREATE TABLE IF NOT EXISTS hive_query_log(
queryid STRING,
sessionid STRING,
querystring STRING,
starttime BIGINT,
endtime BIGINT,
queryretcode INT,
querynumtask INT
)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE;

load data local inpath '/usr/local/hadoop/hivehistoryparser/out/queryLog'  OVERWRITE INTO TABLE  hive_query_log;

CREATE TABLE IF NOT EXISTS hive_task_log(
taskid 	STRING,
queryid STRING,
taskname STRING,
starttime BIGINT,
endtime BIGINT,
taskretcode INT,
taskhadoopid STRING,
tasknummappers INT,
tasknumreducers INT,
taskhadoopprogress STRING,
taskcounters STRING,
taskrowinserted BIGINT
)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\t' 
STORED AS TEXTFILE;

load data local inpath '/usr/local/hadoop/hivehistoryparser/out/taskLog'  OVERWRITE INTO TABLE  hive_task_log;
