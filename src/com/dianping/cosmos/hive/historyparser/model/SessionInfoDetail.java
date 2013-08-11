package com.dianping.cosmos.hive.historyparser.model;

import org.apache.hadoop.hive.ql.history.HiveHistory.SessionInfo;

public class SessionInfoDetail extends SessionInfo {

	private static final String DELIMITER = "\t";

	private String sessionId;
	private String hostIP;
	private String userName;
	private String sessionStartTime;
	private String sessionEndTime;

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getHostIP() {
		return hostIP;
	}

	public void setHostIP(String hostIP) {
		this.hostIP = hostIP;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getSessionStartTime() {
		return sessionStartTime;
	}

	public void setSessionStartTime(String sessionStartTime) {
		this.sessionStartTime = sessionStartTime;
	}

	public String getSessionEndTime() {
		return sessionEndTime;
	}

	public void setSessionEndTime(String sessionEndTime) {
		this.sessionEndTime = sessionEndTime;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(sessionId).append(DELIMITER)
		  .append(hostIP).append(DELIMITER)
		  .append(userName).append(DELIMITER)
		  .append(sessionStartTime).append(DELIMITER)
		  .append(sessionEndTime);
		return sb.toString();
	}

}