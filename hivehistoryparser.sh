#!/bin/bash

cd `dirname "$0"`

host=`hostname`

java -Dhostip=$host -DdatePattern=$1 -jar HiveHistoryParser.jar /data/hive-query-log $1
