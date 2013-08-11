/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dianping.cosmos.hive.historyparser.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.history.HiveHistory.Listener;
import org.apache.hadoop.hive.ql.history.HiveHistory.QueryInfo;
import org.apache.hadoop.hive.ql.history.HiveHistory.RecordTypes;
import org.apache.hadoop.hive.ql.history.HiveHistory.TaskInfo;

import com.dianping.cosmos.hive.historyparser.model.SessionInfoDetail;

/**
 * HiveHistoryViewer.
 * 
 */
public class HiveHistoryViewer implements Listener {
	
	String historyFile;
	SessionInfoDetail sessionInfo = new SessionInfoDetail();

	public static enum ExtendedKeys {
		SESSION_STARTTIME,
		SESSION_ENDTIIME,
		QUERY_STARTTIME,
		QUERY_ENDTIME,
		TASK_STARTTIME,
		TASK_ENDTIME
	}
	// Job Hash Map
	private final HashMap<String, QueryInfo> jobInfoMap = new HashMap<String, QueryInfo>();

	// Task Hash Map
	private final HashMap<String, TaskInfo> taskInfoMap = new HashMap<String, TaskInfo>();

	public HiveHistoryViewer(String path) {
		historyFile = path;
		init();
	}

	public SessionInfoDetail getSessionInfo() {
		return sessionInfo;
	}

	public Map<String, QueryInfo> getJobInfoMap() {
		return jobInfoMap;
	}

	public Map<String, TaskInfo> getTaskInfoMap() {
		return taskInfoMap;
	}

	/**
	 * Parse history files.
	 */
	void init() {

		try {
			HiveHistory.parseHiveHistory(historyFile, this);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Implementation Listner interface function.
	 * 
	 * @see org.apache.hadoop.hive.ql.history.HiveHistory.Listener#handle(org.apache.hadoop.hive.ql.history.HiveHistory.RecordTypes,
	 *      java.util.Map)
	 */
	public void handle(RecordTypes recType, Map<String, String> values) {

		if (recType == RecordTypes.SessionStart) {

			String sessionId = values.get(Keys.SESSION_ID.name());
			sessionInfo.setSessionId(sessionId);
			sessionInfo.setSessionStartTime(Long.parseLong((values.get(Keys.TIME.name())==null?"0":values.get(Keys.TIME.name()))));
		} else if (recType == RecordTypes.SessionEnd) {
			sessionInfo.setSessionEndTime(Long.parseLong(values.get(Keys.TIME.name())==null?"0":values.get(Keys.TIME.name())));
		} else if (recType == RecordTypes.QueryStart) {
			String key = values.get(Keys.QUERY_ID.name());
			QueryInfo ji;
			if (jobInfoMap.containsKey(key)) {
				ji = jobInfoMap.get(key);

				ji.hm.putAll(values);
				ji.hm.put(ExtendedKeys.QUERY_STARTTIME.name(), values.get(Keys.TIME.name()));

			} else {
				ji = new QueryInfo();
				ji.hm = new HashMap<String, String>();
				ji.hm.putAll(values);
				ji.hm.put(ExtendedKeys.QUERY_STARTTIME.name(), values.get(Keys.TIME.name()));

				jobInfoMap.put(key, ji);

			}
		} else if (recType == RecordTypes.QueryEnd) {
			String key = values.get(Keys.QUERY_ID.name());
			QueryInfo ji;
			if (jobInfoMap.containsKey(key)) {
				ji = jobInfoMap.get(key);

				ji.hm.putAll(values);
				ji.hm.put(ExtendedKeys.QUERY_ENDTIME.name(), values.get(Keys.TIME.name()));

			} else {
				ji = new QueryInfo();
				ji.hm = new HashMap<String, String>();
				ji.hm.putAll(values);
				ji.hm.put(ExtendedKeys.QUERY_ENDTIME.name(), values.get(Keys.TIME.name()));

				jobInfoMap.put(key, ji);

			}
		} else if (recType == RecordTypes.TaskStart) {
			String jobid = values.get(Keys.QUERY_ID.name());
			String taskid = values.get(Keys.TASK_ID.name());
			String key = jobid + ":" + taskid;
			TaskInfo ti;
			if (taskInfoMap.containsKey(key)) {
				ti = taskInfoMap.get(key);
				ti.hm.putAll(values);
				ti.hm.put(ExtendedKeys.TASK_STARTTIME.name(), values.get(Keys.TIME.name()));
			} else {
				ti = new TaskInfo();
				ti.hm = new HashMap<String, String>();
				ti.hm.putAll(values);
				ti.hm.put(ExtendedKeys.TASK_STARTTIME.name(), values.get(Keys.TIME.name()));
				taskInfoMap.put(key, ti);

			}

		} else if (recType == RecordTypes.TaskEnd) {
			String jobid = values.get(Keys.QUERY_ID.name());
			String taskid = values.get(Keys.TASK_ID.name());
			String key = jobid + ":" + taskid;
			TaskInfo ti;
			if (taskInfoMap.containsKey(key)) {
				ti = taskInfoMap.get(key);
				ti.hm.putAll(values);
				ti.hm.put(ExtendedKeys.TASK_ENDTIME.name(), values.get(Keys.TIME.name()));
			} else {
				ti = new TaskInfo();
				ti.hm = new HashMap<String, String>();
				ti.hm.putAll(values);
				ti.hm.put(ExtendedKeys.TASK_ENDTIME.name(), values.get(Keys.TIME.name()));
				taskInfoMap.put(key, ti);

			}

		} else if (recType == RecordTypes.TaskProgress) {

			String jobid = values.get(Keys.QUERY_ID.name());
			String taskid = values.get(Keys.TASK_ID.name());
			String key = jobid + ":" + taskid;
			TaskInfo ti;
			if (taskInfoMap.containsKey(key)) {
				ti = taskInfoMap.get(key);
				ti.hm.putAll(values);
			} else {
				ti = new TaskInfo();
				ti.hm = new HashMap<String, String>();
				ti.hm.putAll(values);
				taskInfoMap.put(key, ti);

			}

		}

	}

}
