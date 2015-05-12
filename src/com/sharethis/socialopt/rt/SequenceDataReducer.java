package com.sharethis.socialopt.rt;

import java.io.IOException;
import java.util.ArrayList;
// import java.util.Collections;
import java.util.HashMap;
// import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;
// import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.sharethis.socialopt.common.Constants;
import com.sharethis.socialopt.common.STDateUtils;
import com.sharethis.socialopt.common.Utils;
// import com.sharethis.socialopt.common.STDateUtils;

public class SequenceDataReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_RT_LOGGER_NAME);	

	private Pattern p_cmpn = Pattern.compile("\"cmpn\":\"(\\w+?)\"");
	private Pattern p_date = Pattern.compile("\"date\":\"(\\.+?)\"");
	
	private HashMap<String, List<String>> hm_pixel_ids;
	private HashMap<String, String> hmCat;
	private HashMap<String, String> hmMoat;
	private Text ReducerKey = new Text();
	private Text ReducerVal = new Text();
	private MultipleOutputs<Text, Text> mos;
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Sequence Impressions and Conversions Reducer Starts.");
		
		mos = new MultipleOutputs<Text, Text>(context);
		hm_pixel_ids = new HashMap<String, List<String>>();
		hmCat = new HashMap<String, String>();
		hmMoat = new HashMap<String, String>();

		Configuration conf = context.getConfiguration();
		String str_pixels = conf.get("pixels");
		if (str_pixels != null && (!str_pixels.isEmpty())) {
			StringTokenizer st = new StringTokenizer(str_pixels, ",");
			while (st.hasMoreTokens()) {
				String[] tokens = st.nextToken().split("\\|");
				if (tokens.length >= 3) {
					if (tokens[2].equals("1") || tokens[2].equals("7")) {
						List<String> ids;
						if (hm_pixel_ids.containsKey(tokens[0]))
							ids = hm_pixel_ids.get(tokens[0]);
						else
							ids = new ArrayList<String>();
						if (ids != null) {
							ids.add(tokens[1]);
							hm_pixel_ids.put(tokens[0], ids);
						}
						hmCat.put(tokens[0], tokens[2]);
					}
					else if (tokens[2].equals("8"))
						hmMoat.put(tokens[0], tokens[1]);
				}
			}
		}
	}
	
	protected String get_pattern(String seq, Pattern p) {
		String value = "";
		Matcher m = p.matcher(seq);
		if (m.find() && m.groupCount() > 0)
			value = m.group(1);
		if (value == null || value.isEmpty() || value.equalsIgnoreCase("unknown") || value.equalsIgnoreCase("null"))
			value = "-";
		return value;
	}
	
	protected String getKey(String rec) {
		String[] tokens = rec.split("\t");
		if (tokens != null && tokens.length >= 1)
			return tokens[0];
		else
			return "-";
	}
	
	protected String getValue(String rec) {
		String[] tokens = rec.split("\t");
		if (tokens == null)
			return "-";
		
		int n = tokens.length;
		String ret = "";
		for (int i = 1; i < n; i++)
			ret += (ret.isEmpty() ? tokens[i] : ("\t" + tokens[i]));
		return ret;
	}
	
	protected String getID(String rec) {
		String[] tokens = rec.split("\t");
		if (tokens != null && tokens.length >= Constants.FIELDS.length)
			return tokens[13];
		else
			return "-";
	}
	
	protected String getString(HashMap<String, String> hm) {
		if (hm == null)
			return "";
		String ret = "";
		for (int i = 0; i < Constants.FIELDS.length; i++) {
			String value = "-";
			if (hm.containsKey(Constants.FIELDS[i]))
				value = hm.get(Constants.FIELDS[i]);
			ret += (ret.isEmpty() ? value : ("\t" + value));
		}
		return ret;
	}
	
	protected String getObjString(String s) {
		JSONObject obj = new JSONObject(s);
		JSONObject target = new JSONObject();
		if (obj.has("dwellTime") && !obj.isNull("dwellTime"))
			target.put("dwellTime", obj.get("dwellTime"));
		if (obj.has("didScroll") && !obj.isNull("didScroll"))
			target.put("didScroll", obj.get("didScroll"));
		if (obj.has("timeToScroll") && !obj.isNull("timeToScroll"))
			target.put("timeToScroll", obj.get("timeToScroll"));
		if (obj.has("scrollDepth") && !obj.isNull("scrollDepth"))
			target.put("scrollDepth", obj.get("scrollDepth"));
		if (obj.has("scrollUpMs") && !obj.isNull("scrollUpMs"))
			target.put("scrollUpMs", obj.get("scrollUpMs"));
		if (obj.has("scrollDownMs") && !obj.isNull("scrollDownMs"))
			target.put("scrollDownMs", obj.get("scrollDownMs"));
		if (obj.has("scrollDownPx") && !obj.isNull("scrollDownPx"))
			target.put("scrollDownPx", obj.get("scrollDownPx"));
		if (obj.has("scrollUpPx") && !obj.isNull("scrollUpPx"))
			target.put("scrollUpPx", obj.get("scrollUpPx"));
		if (obj.has("cpid") && !obj.isNull("cpid"))
			target.put("cpid", obj.get("cpid"));
		return target.toString();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		TreeMap<String, String> tmConv = new TreeMap<String, String>();
		TreeMap<String, String> tmMoat = new TreeMap<String, String>();
		TreeMap<String, String> tmImp = new TreeMap<String, String>();
		
		for (Text value: values) {
			String[] items = value.toString().split("\t");
			int n = items.length;
			if (n <= 1)
				continue;
			else if (items[0].equals("CONV") || items[0].equals("MOAT")) {
				for (int i = 1; i < n; i++) {
					String date = get_pattern(items[i], p_date);
					if (items[0].equals("CONV")) {
						String cmpn = get_pattern(items[i], p_cmpn);
						tmConv.put(date, cmpn);
					}
					else {	// MOAT data
						String engagement = getObjString(items[i]);
						tmMoat.put(date, engagement);
					}
				}
			}
			else if (items[0].equals("IMP")) {
				String date = STDateUtils.format(items[11]);
				tmImp.put(date, Utils.join("\t", items, 1, n - 1));
			}
		}
		
		// Assign conversions
		for (String key_conv_date: tmConv.keySet()) {
			String pixel = tmConv.get(key_conv_date);
			String type = "0";
			if (hmCat.containsKey(pixel))
				type = hmCat.get(pixel);
			String prev_date = "";
			for (String key_imp_date: tmImp.keySet()) {
				if (key_conv_date.compareTo(key_imp_date) >= 0)
					break;
				else {
					String[] items = tmImp.get(key_imp_date).split("\t");
					int n = items.length;
					if (n <= 0)
						continue;
					else if (items[n - 4].equals("0"))
						continue;
					else if (hm_pixel_ids.containsKey(pixel) && hm_pixel_ids.get(pixel).contains(items[13])) 
						prev_date = key_imp_date;
					else
						continue;
				}
			}
			if (!prev_date.isEmpty()) {
				String[] items = tmImp.get(prev_date).split("\t");
				int n = items.length;
				if (items[n - 3].equals("-"))
					items[n - 3] = pixel + "," + type;
				else
					items[n - 3] = items[n - 3] + "|" + pixel + "," + type;
				tmImp.put(prev_date, Utils.join("\t", items, 0, n - 1));
			}
		}
		
		// Assign MOAT
		for (String key_moat_date: tmMoat.keySet()) {
			String engagement = tmMoat.get(key_moat_date);
			JSONObject obj = new JSONObject(engagement);	
			String prev_date = "";
			for (String key_imp_date: tmImp.keySet()) {
				if (key_moat_date.compareTo(key_imp_date) >= 0)
					break;
				else {
					String[] items = tmImp.get(key_imp_date).split("\t");
					int n = items.length;
					if (n <= 0)
						continue;
					else if (obj.has("cpid") && hmMoat.containsKey(obj.get("cpid")) && hmMoat.get(obj.get("cpid")).equals(items[13])) 
						prev_date = key_imp_date;
					else
						continue;
				}
			}
			if (!prev_date.isEmpty()) {
				String[] items = tmImp.get(prev_date).split("\t");
				int n = items.length;
				if (items[n - 1].equals("-"))
					items[n - 1] = engagement;
				else
					items[n - 1] = items[n - 1] + "|" + engagement;
				tmImp.put(prev_date, Utils.join("\t", items, 0, n - 1));
			}
		}
		
		// Output
		for (String key_date: tmImp.keySet()) {
			String record = tmImp.get(key_date);
			ReducerKey.set(getKey(record));
			ReducerVal.set(getValue(record));
			String file = key_date.substring(0, 8);
			mos.write(ReducerKey, ReducerVal, file + "/part");
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
		logger.info("Sequence Impressions and Conversions Completed.");
	}
}