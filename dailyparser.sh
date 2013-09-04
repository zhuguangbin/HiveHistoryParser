#!/bin/bash
source /etc/profile
sh ~/getticket.sh

HIVEHISTORYPARSER_HOME=/usr/local/hadoop/hivehistoryparser/
host=`hostname`
lastday=`date -d last-day +%Y%m%d`

echo '--------------------------------------------'
echo 'start parsing hive history log of '$lastday

sh $HIVEHISTORYPARSER_HOME/hivehistoryparser.sh $lastday > $HIVEHISTORYPARSER_HOME/parse.log.$lastday 2>&1
echo "parse done!"

# put result to HDFS
echo 'put result to HDFS'
sh ~/getticket.sh
hadoop fs -put $HIVEHISTORYPARSER_HOME/out/sessionLog_*_$lastday /user/hive/warehouse/hive_session_log/
hadoop fs -put $HIVEHISTORYPARSER_HOME/out/queryLog_*_$lastday   /user/hive/warehouse/hive_query_log/
hadoop fs -put $HIVEHISTORYPARSER_HOME/out/taskLog_*_$lastday    /user/hive/warehouse/hive_task_log/
echo 'result putted to HDFS'
echo '---------------------------------------------'
