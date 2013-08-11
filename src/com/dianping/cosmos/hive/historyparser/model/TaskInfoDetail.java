package com.dianping.cosmos.hive.historyparser.model;

public class TaskInfoDetail {
	
	private static final String DELIMITER = "\t";
	
	private String queryId;
	private String taskId;
	private String taskName;
	private long taskStartTime;
	private long taskEndTime;
	private int taskRetCode;
	private String taskHadoopId;
	private String taskHadoopProgress;
	private int taskNumMappers;
	private int taskNumReducers;
	private String taskCounters;
	private long taskRowInserted;

	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public long getTaskStartTime() {
		return taskStartTime;
	}

	public void setTaskStartTime(long taskStartTime) {
		this.taskStartTime = taskStartTime;
	}

	public long getTaskEndTime() {
		return taskEndTime;
	}

	public void setTaskEndTime(long taskEndTime) {
		this.taskEndTime = taskEndTime;
	}

	public int getTaskRetCode() {
		return taskRetCode;
	}

	public void setTaskRetCode(int taskRetCode) {
		this.taskRetCode = taskRetCode;
	}

	public String getTaskHadoopId() {
		return taskHadoopId;
	}

	public void setTaskHadoopId(String taskHadoopId) {
		this.taskHadoopId = taskHadoopId;
	}

	public String getTaskHadoopProgress() {
		return taskHadoopProgress;
	}

	public void setTaskHadoopProgress(String taskHadoopProgress) {
		this.taskHadoopProgress = taskHadoopProgress;
	}

	public int getTaskNumMappers() {
		return taskNumMappers;
	}

	public void setTaskNumMappers(int taskNumMappers) {
		this.taskNumMappers = taskNumMappers;
	}

	public int getTaskNumReducers() {
		return taskNumReducers;
	}

	public void setTaskNumReducers(int taskNumReducers) {
		this.taskNumReducers = taskNumReducers;
	}

	public String getTaskCounters() {
		return taskCounters;
	}

	public void setTaskCounters(String taskCounters) {
		this.taskCounters = taskCounters;
	}

	public long getTaskRowInserted() {
		return taskRowInserted;
	}

	public void setTaskRowInserted(long taskRowInserted) {
		this.taskRowInserted = taskRowInserted;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(taskId).append(DELIMITER)
		  .append(queryId).append(DELIMITER)
		  .append(taskName).append(DELIMITER)
		  .append(taskStartTime).append(DELIMITER)
		  .append(taskEndTime).append(DELIMITER)
		  .append(taskRetCode).append(DELIMITER)
		  .append(taskHadoopId).append(DELIMITER)
		  .append(taskNumMappers).append(DELIMITER)
		  .append(taskNumReducers).append(DELIMITER)
		  .append(taskHadoopProgress).append(DELIMITER)
		  .append(taskCounters).append(DELIMITER)
		  .append(taskRowInserted);
		return sb.toString();
	}
	
}
