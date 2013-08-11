package com.dianping.cosmos.hive.historyparser.model;

import java.util.Map;

public class QueryInfoDetail {
	
	private static final String DELIMITER = "\t";

	private String queryId;
	private String sessionId;
	private String queryString;
	private long queryStartTime;
	private long queryEndTime;
	private int queryRetCode;
	private int queryNumTask;
	private Map<String, Long> rowCounters;

	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public long getQueryStartTime() {
		return queryStartTime;
	}

	public void setQueryStartTime(long queryStartTime) {
		this.queryStartTime = queryStartTime;
	}

	public long getQueryEndTime() {
		return queryEndTime;
	}

	public void setQueryEndTime(long queryEndTime) {
		this.queryEndTime = queryEndTime;
	}

	public int getQueryRetCode() {
		return queryRetCode;
	}

	public void setQueryRetCode(int queryRetCode) {
		this.queryRetCode = queryRetCode;
	}

	public int getQueryNumTask() {
		return queryNumTask;
	}

	public void setQueryNumTask(int queryNumTask) {
		this.queryNumTask = queryNumTask;
	}
	
	public Map<String, Long> getRowCounters() {
		return rowCounters;
	}

	public void setRowCounters(Map<String, Long> rowCounters) {
		this.rowCounters = rowCounters;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(queryId).append(DELIMITER)
		  .append(sessionId).append(DELIMITER)
		  .append(queryString).append(DELIMITER)
		  .append(queryStartTime).append(DELIMITER)
		  .append(queryEndTime).append(DELIMITER)
		  .append(queryRetCode).append(DELIMITER)
		  .append(queryNumTask).append(DELIMITER);
		return sb.toString();
	}

}
