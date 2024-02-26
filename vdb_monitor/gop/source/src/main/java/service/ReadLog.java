package service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Set;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import model.Config;
import model.Data;
import model.ResultCommon;

public class ReadLog {

	public LinkedHashMap<LocalDateTime, ResultCommon[]> timeMap = new LinkedHashMap<LocalDateTime, ResultCommon[]>();
	public LinkedHashMap<LocalDateTime, ResultCommon[]> rangeTimeMap = new LinkedHashMap<LocalDateTime, ResultCommon[]>();
	public LinkedHashMap<LocalDateTime, ResultCommon[]> nameMap = new LinkedHashMap<LocalDateTime, ResultCommon[]>();
	public LinkedHashMap<LocalDateTime, ResultCommon[]> tagMap = new LinkedHashMap<LocalDateTime, ResultCommon[]>();

	public ReadLog(File file, Gson gson, Config config) throws JsonSyntaxException, IOException, ParseException {
		super();
		File rFile = file;

		FileInputStream fis = new FileInputStream(rFile);
		InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(isr);

		String line = "";

		int i = 0;
		ResultCommon[] rc = new ResultCommon[config.measure.length];

		while ((line = reader.readLine()) != null) {
			Data obj = gson.fromJson(line, Data.class);
			LocalDateTime ts = stringToTimestamp(obj.time, gson);
			// if (i >= config.common.length) {
			if (i >= config.measure.length) {
				rc = new ResultCommon[config.measure.length];
				i = 0;
			}
			// rc[i] = obj.rc[i];
			rc = obj.rc;
			timeMap.put(ts, rc);
			i++;
		}
	}

	public String convString(LinkedHashMap<LocalDateTime, ResultCommon[]> map) {
		Set<LocalDateTime> timeKeys = map.keySet();
		String st = "";
		for (LocalDateTime key : timeKeys) {
			ResultCommon[] rc = timeMap.get(key);
			for (int i = 0; i < rc.length; i++) {
				st += key + " -- " + rc[i].toString() + "\n";
			}
		}
		return st;
	}

	private LocalDateTime stringToTimestamp(String timestamp, Gson gson) {
		// TODO Auto-generated method stub
		String tempTime = gson.fromJson(timestamp, String.class);
		DateTimeFormatter formatDateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
		LocalDateTime localDateTime = LocalDateTime.from(formatDateTime.parse(tempTime));
		return localDateTime;
	}

	public void setRangeTimeMap(LocalDateTime stTs, LocalDateTime edTs) {
		// boolean exit = true;
		Set<LocalDateTime> timeKeys = timeMap.keySet();
		for (LocalDateTime key : timeKeys) {
			if (key.isAfter(stTs) || key.isEqual(stTs)) {
				if (key.isBefore(edTs) || key.isEqual(edTs)) {
//					exit = false;
					rangeTimeMap.put(key, timeMap.get(key));
				}
			}
//			if (exit) {
			// System.out.println("time start :" + stTs + " end : "+ edTs+" is not valid");
			// System.exit(0);
			// }

		}
	}

	public void setNameMap(String measure) {
		boolean exit = true;
		Set<LocalDateTime> timeKeys = timeMap.keySet();
		for (LocalDateTime key : timeKeys) {
			ResultCommon[] rc = timeMap.get(key);
			ResultCommon[] tempRc = new ResultCommon[1];
			for (int i = 0; i < rc.length; i++) {
				if (rc[i].measure.equals(measure)) {
					tempRc[0] = rc[i];
					exit = false;
				}
				;
			}
			if (exit) {
				System.out.println("measure :" + measure + " is not valid");
				System.exit(0);
			}
			nameMap.put(key, tempRc);
		}
	}

	public void setTagMap(String tag) {
		boolean exit = true;
		Set<LocalDateTime> timeKeys = timeMap.keySet();
		for (LocalDateTime key : timeKeys) {
			ResultCommon[] rc = timeMap.get(key);
			for (int i = 0; i < rc.length; i++) {
				if (rc[i] != null) {
					if (rc[i].tag.equals(tag) == false) {
						rc[i] = null;
					} else {
						exit = false;
					}
				} else {
					// System.out.println(key);
				}
			}
			if (exit) {
				System.out.println("tag :" + tag + " is not valid");
				System.exit(0);
			}
			tagMap.put(key, rc);
		}
	}
}
