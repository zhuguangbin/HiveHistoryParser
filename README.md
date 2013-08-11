## Hive History Parser

### What is it ?

  Hive History Parse parses hive query log and output three important informations to log:

1. sessionlog: output all session information on each hive client. 
log format:
[SESSIONID]	[HOSTIP]	[USERNAME]	[SESSIONSTARTTIME]	[SESSIONENDTIME]

2. querylog: output all query information per session.
log format:
[QUERYID]	[SESSIONID]	[QUERYSTRING]	[QUERYSTARTTIME]	[QUERYENDTIME]	[QUERYRETCODE]	[QUERYNUMTASK]

3. tasklog: output all task information per query.
log format:
[TASKID]	[QUERYID]	[TASKNAME]	[TASKSTARTTIME]	[TASKENDTIME]	[TASKRETCODE]	[TASKHADOOPID]	[TASKNUMMAPPERS]	[TASKNUMREDUCERS]	[TASKHADOOPPROGRESS]	[TASKCOUNTERS]	[TASKROWINSERTED]

### How to use it ?

1. git clone 

2. import project to eclipse

3. export runnable jar

4. java -jar HiveHistoryParser.jar
   
   results are three tsv file in the "out" directory. You can load it to hive for analysis. Hive table ddl sql in sql/hive_history.hql


