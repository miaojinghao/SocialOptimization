package com.sharethis.socialopt.common;

public class Utils {
	public static String join(String sep, String[] items, int start, int end) {
		String ret = "";
		for (int i = start; i <= end && i < items.length; i++)
			ret += (ret.isEmpty() ? items[i] : (sep + items[i]));
		return ret;
	}
}