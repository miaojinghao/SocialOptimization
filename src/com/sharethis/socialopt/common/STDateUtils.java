package com.sharethis.socialopt.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;

import org.apache.commons.lang3.time.DateUtils;

/**
 * The class contains a set of date utility functions
 * 
 * @author Jinghao Miao
 * Version 1.0
 * 2015-01-21
 * 
 */

public class STDateUtils {
	
	public static String getNextDay(String dateString) {
		try {
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			Date inDate = df.parse(dateString);
			Date retDate = DateUtils.addDays(inDate, 1);
			return df.format(retDate);
		} catch (IllegalArgumentException e) {
			return "";
		} catch (ParseException e) {
			return "";
		}
	}
	
	public static String getPreviousDay(String dateString) {
		try {
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			Date inDate = df.parse(dateString);
			Date retDate = DateUtils.addDays(inDate, -1);
			return df.format(retDate);
		} catch (IllegalArgumentException e) {
			return "";
		} catch (ParseException e) {
			return "";
		}
	}
	
	public static String getAddDays(String dateString, int nDays) {
		if (nDays == 0)
			return dateString;
		
		try {
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			Date inDate = df.parse(dateString);
			Date retDate = DateUtils.addDays(inDate, nDays);
			return df.format(retDate);
		} catch (IllegalArgumentException e) {
			return "";
		} catch (ParseException e) {
			return "";
		}
	}
	
	public static String format(String ms) {
		try {
			long unix_ms = Long.parseLong(ms);
			Date dt = new Date(unix_ms);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			return sdf.format(dt);
		} catch (NumberFormatException e) {
			return "";
		}
	}
	
	public static String getHour(String ms) {
		String date = format(ms);
		if (date != null && !date.isEmpty() && date.length() >= 17) {
			String hour = date.substring(9, 11);
			return hour;
		}
		else
			return "00";
	}
	
	public static String getDate(String ms) {
		String date = format(ms);
		if (date != null && !date.isEmpty() && date.length() >= 17) {
			String dt = date.substring(0, 8);
			return dt;
		}
		else
			return "00000000";
	}
	
	public static String getDayOfWeek(String ms) {
		try {
			Calendar cal = Calendar.getInstance();
			long unix_ms = Long.parseLong(ms);
			Date dt = new Date(unix_ms);
			cal.setTime(dt);
			return String.valueOf(cal.get(Calendar.DAY_OF_WEEK));
		} catch (NumberFormatException e) {
			return "1";
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(STDateUtils.format("1430180037009"));
	}
}