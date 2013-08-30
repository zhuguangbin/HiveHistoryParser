package com.dianping.cosmos.hive.historyparser;

import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class Main {

	private static final Logger log = Logger.getLogger(Main.class);
	

	public static void printUsage() {
		System.err.println("Usage: Main <hive-query-log-dir> [YYYYMMDDhhmm]");
		System.err.println("example: ");
		System.err.println("        Main /data/hive-query-log 201308");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length < 1 || args.length > 2) {
			printUsage();
			System.exit(1);
		} else {
			File hiveQueryLogDir = new File(args[0]);
			String datePattern = "............";
			if (args.length==2) {
				if (args[1].length()<=12) {
					datePattern = StringUtils.rightPad(args[1], 12, ".");
				}else {
					System.err.println("date format error : length > 12");
					printUsage();
					System.exit(1);
				}
			}
			if (!hiveQueryLogDir.isDirectory()) {
				System.err
						.println(args[0]
								+ " is not a valid directory, Please give me the hive-query-log directory");
				System.exit(1);
			} else {
				System.err.println("-------------------------Start parsing , date pattern: "+ datePattern +"----------------");
				File[] hiveQueryLogDirPerUsers = hiveQueryLogDir.listFiles();
				for (File userHiveQueryLogDir : hiveQueryLogDirPerUsers) {
					if (!userHiveQueryLogDir.isDirectory()) {
						System.err.println(userHiveQueryLogDir.getAbsolutePath()
								+ " is not a user hive-query-log directory!");
						log.error(userHiveQueryLogDir.getAbsolutePath()	+ " is not a user hive-query-log directory!");
					} else {
						new HiveHistoryParser(userHiveQueryLogDir, datePattern).start();
					}
				}
			}
			
		}
	}

}
