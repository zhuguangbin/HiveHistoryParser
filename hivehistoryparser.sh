#!/bin/bash

cd `dirname "$0"`

host=`hostname`
source /etc/profile

# start parsing
echo 'start parsing hive history log'
java -Dhostip=$host -DdatePattern=$1 -jar HiveHistoryParser.jar /data/hive-query-log $1
echo 'hive history log parsing done'

