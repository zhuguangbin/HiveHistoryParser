package com.dianping.cosmos.hive.historyparser.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.history.HiveHistory.QueryInfo;
import org.apache.hadoop.hive.ql.history.HiveHistory.TaskInfo;
import org.apache.log4j.Logger;

import com.dianping.cosmos.hive.historyparser.main.HiveHistoryViewer.ExtendedKeys;
import com.dianping.cosmos.hive.historyparser.model.QueryInfoDetail;
import com.dianping.cosmos.hive.historyparser.model.SessionInfoDetail;
import com.dianping.cosmos.hive.historyparser.model.TaskInfoDetail;

public class HiveHistoryParser {

	private static final Logger log = Logger.getLogger(HiveHistoryParser.class);
	private static final Logger sessionLog = Logger.getLogger("SessionLog");
	private static final Logger queryLog = Logger.getLogger("QueryLog");
	private static final Logger taskLog = Logger.getLogger("TaskLog");
	
	private static long numParsedDir = 0;
	private static long numParsedFile = 0;

	public static void parseOneFile(File logFile) {

		String userName = null;

		String fileName = logFile.getName();
		String[] fileNameComponent = fileName.split("_");
		userName = fileNameComponent[3];
		HiveHistoryViewer hw = null;

		try {
			hw = new HiveHistoryViewer(logFile.getAbsolutePath());
		} catch (Exception e) {
			// TODO: handle exception
			if (e instanceof FileNotFoundException) {
				log.error("File Not found : "
						+ logFile.getAbsolutePath() + ", " + e.getMessage());
			} else {
				log.error("unknown exception while parsing file " +logFile.getAbsolutePath()+" :  "+ e.getMessage());
				e.printStackTrace();
			}
		}

		// parse session info
		SessionInfoDetail sessionInfo = hw.getSessionInfo();
		sessionInfo.setUserName(userName);
		String hostIP = null;
		try {
			hostIP = InetAddress.getLocalHost().toString();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			log.error("cannot get localhost ip : " + e.getMessage());
			e.printStackTrace();
		}
		sessionInfo.setHostIP(hostIP);
		
		sessionLog.info(sessionInfo);

		// parse query info
		Map<String, QueryInfo> jobinfoMap = hw.getJobInfoMap();
		for (String query_id : jobinfoMap.keySet()) {

			QueryInfoDetail query_info_detail = new QueryInfoDetail();

			QueryInfo query_info = jobinfoMap.get(query_id);
			Map<String, String> query_info_map = query_info.hm;
			Map<String, Long> query_info_counter = query_info.rowCountMap;

			query_info_detail.setQueryId(query_id);
			query_info_detail.setSessionId(sessionInfo.getSessionId());
			query_info_detail.setQueryString(query_info_map
					.get(Keys.QUERY_STRING.name()));
			query_info_detail.setQueryStartTime(Long.parseLong(query_info_map
					.get(ExtendedKeys.QUERY_STARTTIME.name())));
			query_info_detail.setQueryEndTime(Long.parseLong(query_info_map
					.get(ExtendedKeys.QUERY_ENDTIME.name()) == null ? "0"
					: query_info_map.get(ExtendedKeys.QUERY_ENDTIME.name())));
			query_info_detail.setQueryRetCode(Integer.parseInt(query_info_map
					.get(Keys.QUERY_RET_CODE.name()) == null ? "1"
					: query_info_map.get(Keys.QUERY_RET_CODE.name())));
			query_info_detail.setQueryNumTask(Integer.parseInt(query_info_map
					.get(Keys.QUERY_NUM_TASKS.name()) == null ? "0"
					: query_info_map.get(Keys.QUERY_NUM_TASKS.name())));
			query_info_detail.setRowCounters(query_info_counter);
			
			queryLog.info(query_info_detail);

		}

		// parse task info
		Map<String, TaskInfo> taskinfos = hw.getTaskInfoMap();
		for (String task_id : taskinfos.keySet()) {
			TaskInfoDetail task_info_detail = new TaskInfoDetail();

			TaskInfo taskinfo = taskinfos.get(task_id);
			Map<String, String> taskinfoMap = taskinfo.hm;

			task_info_detail.setTaskId(task_id);
			task_info_detail
					.setTaskName(taskinfoMap.get(Keys.TASK_NAME.name()));
			task_info_detail.setQueryId(taskinfoMap.get(Keys.QUERY_ID.name()));
			task_info_detail.setTaskStartTime(Long.parseLong(taskinfoMap
					.get(ExtendedKeys.TASK_STARTTIME.name())));
			task_info_detail.setTaskEndTime(Long.parseLong(taskinfoMap
					.get(ExtendedKeys.TASK_ENDTIME.name()) == null ? "0"
					: taskinfoMap.get(ExtendedKeys.TASK_ENDTIME.name())));
			task_info_detail.setTaskRetCode(Integer.parseInt(taskinfoMap
					.get(Keys.TASK_RET_CODE.name()) == null ? "1" : taskinfoMap
					.get(Keys.TASK_RET_CODE.name())));
			task_info_detail.setTaskHadoopId(taskinfoMap
					.get(Keys.TASK_HADOOP_ID.name()));
			task_info_detail.setTaskNumMappers(Integer.parseInt(taskinfoMap
					.get(Keys.TASK_NUM_MAPPERS.name()) == null ? "0"
					: taskinfoMap.get(Keys.TASK_NUM_MAPPERS.name())));
			task_info_detail.setTaskNumReducers(Integer.parseInt(taskinfoMap
					.get(Keys.TASK_NUM_REDUCERS.name()) == null ? "0"
					: taskinfoMap.get(Keys.TASK_NUM_REDUCERS.name())));
			task_info_detail.setTaskHadoopProgress(taskinfoMap
					.get(Keys.TASK_HADOOP_PROGRESS.name()));
			task_info_detail.setTaskCounters(taskinfoMap.get(Keys.TASK_COUNTERS
					.name()));
			task_info_detail.setTaskRowInserted(Long.parseLong(taskinfoMap
					.get(Keys.ROWS_INSERTED.name()) == null ? "0" : taskinfoMap
					.get(Keys.ROWS_INSERTED.name())));

			taskLog.info(task_info_detail);

		}

	}

	public static void printUsage() {
		System.err.println("HiveHistoryParser <hive-query-log-dir>");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 1) {
			printUsage();
			System.exit(1);
		} else {
			File hiveQueryLogDir = new File(args[0]);
			if (!hiveQueryLogDir.isDirectory()) {
				System.err
						.println(args[0]
								+ " is not a valid directory, Please give me the hive-query-log directory");
				System.exit(1);
			} else {
				System.err.println("Start parsing Hive Query Log in dir :" + args[0]);
				File[] hiveQueryLogDirPerUsers = hiveQueryLogDir.listFiles();
				for (File userHiveQueryLogDir : hiveQueryLogDirPerUsers) {
					if (!userHiveQueryLogDir.isDirectory()) {
						System.err.println(userHiveQueryLogDir.getAbsolutePath()
								+ " is not a user hive-query-log directory!");
						log.error(userHiveQueryLogDir.getAbsolutePath()	+ " is not a user hive-query-log directory!");
					} else {
						String dirName = userHiveQueryLogDir.getName();
						System.err.println("parsing log in dir: "+ dirName);
						File[] hiveQueryLogs = userHiveQueryLogDir.listFiles();
						for (File logFile : hiveQueryLogs) {
							if (logFile.isFile() && logFile.canRead()) {
								
								String regPattern = "hive_job_log_(.*)_(\\d){12}_(\\d)*.txt";
								Pattern pattern = Pattern.compile(regPattern);
								Matcher matcher = pattern.matcher(logFile.getName());
								if (!matcher.matches()) {
									System.err.println(logFile.getAbsolutePath() + " is not a valid hive query log file");
									log.error(logFile.getAbsolutePath() + " is not a valid hive query log file");
								} else {
									try {
										parseOneFile(logFile);
									} catch (Exception e) {
										// TODO: handle exception
										log.error("ERROR while parsing file: "+ logFile.getAbsolutePath());
									}
									
									numParsedFile++;
									
									if (numParsedFile %100 ==0) {
										System.err.println(numParsedFile + " file parsed.");
									}
									
								}
							} else {
								System.err.println(logFile.getAbsolutePath()
										+ " is not a readable log file");
								log.error(logFile.getAbsolutePath()	+ " is not a readable log file");
							}
						}
						
						numParsedDir++;
						if (numParsedDir%10==0) {
							System.err.println(numParsedDir + " dir parsed.");
						}
					}
				}
			}
			
			System.err.println("Summary: "+ numParsedDir + " dir , " + numParsedFile +" file parsed .");
		}
	}

}
