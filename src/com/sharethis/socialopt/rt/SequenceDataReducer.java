package com.sharethis.socialopt.rt;

import java.io.IOException;
import java.util.ArrayList;
// import java.util.Collections;
import java.util.HashMap;
// import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
// import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.sharethis.socialopt.common.Constants;
import com.sharethis.socialopt.common.Utils;
// import com.sharethis.socialopt.common.STDateUtils;

public class SequenceDataReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_RT_LOGGER_NAME);	
	// private Pattern p_date = Pattern.compile("\"date\":\"(.+?)\"");
	// private Pattern p_hour = Pattern.compile("\\s(\\d\\d?):.+?");
	private Pattern p_cmpn = Pattern.compile("\"cmpn\":\"(\\w+?)\"");
	private HashMap<String, List<String>> hm_pixel_ids;
	private HashMap<String, String> hmCat;
	private Text ReducerKey = new Text();
	private Text ReducerVal = new Text();
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Sequence Impressions and Conversions Reducer Starts.");
		
		hm_pixel_ids = new HashMap<String, List<String>>();
		hmCat = new HashMap<String, String>();
		
		Configuration conf = context.getConfiguration();
		String str_pixels = conf.get("pixels");
		if (str_pixels != null && (!str_pixels.isEmpty())) {
			StringTokenizer st = new StringTokenizer(str_pixels, ",");
			while (st.hasMoreTokens()) {
				String[] tokens = st.nextToken().split("\\|");
				if (tokens.length >= 3) {
					List<String> ids;
					if (hm_pixel_ids.containsKey(tokens[0]))
						ids = hm_pixel_ids.get(tokens[0]);
					else
						ids = new ArrayList<String>();
					if (ids != null) {
						ids.add(tokens[1]);
						hm_pixel_ids.put(tokens[0], ids);
					}
					// hmCat.put(tokens[1], tokens[2]);
					hmCat.put(tokens[0], tokens[2]);
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
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashMap<String, HashMap<String, String>> mapIMP = new HashMap<String, HashMap<String, String>>();
		List<String> convs = new ArrayList<String>();
		
		for (Text value: values) {
			String[] items = value.toString().split("\t");
			int n = items.length;
			if (n <= 1)
				continue;
			else if (items[0].equals("IMP") && items[n - 1].equals("0")) {
				ReducerKey.set(items[1]);
				ReducerVal.set(Utils.join("\t", items, 2, n - 1) + "\t-");
				context.write(ReducerKey, ReducerVal);
			}
			else {
				String str_tag = items[0];
				if (str_tag.equals("CONV")) {
					for (int i = 0; i < n; i++) {
						String cmpn = get_pattern(items[i], p_cmpn);
						convs.add(cmpn);
					}					
				}
				else if (str_tag.equals("IMP")) {
					HashMap<String, String> hm = new HashMap<String, String>();
					for (int i = 1; i < n && i < Constants.FIELDS.length + 1; i++) {
						String item = items[i];
						if (item.isEmpty() || item.equalsIgnoreCase("unknown") ||item.equalsIgnoreCase("null"))
							item = "-";
						hm.put(Constants.FIELDS[i - 1], item);
					}
					hm.put("conv_enabled", items[n - 1]);
					hm.put("conversion", "-");
					
					String cmpn_id = "-";
					if (hm.containsKey("campaign_id"))
						cmpn_id = hm.get("campaign_id");
					
					if (mapIMP.containsKey(cmpn_id)) {
						String old_ts = mapIMP.get(cmpn_id).get("timestamp");
						String ts = hm.get("timestamp");
						if (ts.compareTo(old_ts) >= 0) {
							String record = getString(mapIMP.get(cmpn_id));
							String imp_k = getKey(record);
							String imp_v = getValue(record);
							ReducerKey.set(imp_k);
							ReducerVal.set(imp_v);
							context.write(ReducerKey, ReducerVal);
							mapIMP.put(cmpn_id, hm);
						}
						else {
							String record = getString(hm);
							String imp_k = getKey(record);
							String imp_v = getValue(record);
							ReducerKey.set(imp_k);
							ReducerVal.set(imp_v);
							context.write(ReducerKey, ReducerVal);
						}
					}
					else
						mapIMP.put(cmpn_id, hm);
				}
			}
		}
		if (!mapIMP.isEmpty()) {
			for (String cmpn: convs) {
				for (String cmpn_id: mapIMP.keySet()) {
					if (hm_pixel_ids.containsKey(cmpn) && hm_pixel_ids.get(cmpn).contains(cmpn_id)) {
						String cmpn_type = "-1";
						String conv = mapIMP.get(cmpn_id).get("conversion");
						if (hmCat.containsKey(cmpn))
							cmpn_type = hmCat.get(cmpn);
						if (conv.equals("-"))
							conv = cmpn + "," + cmpn_type;
						else
							conv += ("|" + cmpn + "," + cmpn_type);
						mapIMP.get(cmpn_id).put("conversion", conv);
					}
				}
			}
			for (String cmpn_id: mapIMP.keySet()) {
				HashMap<String, String> hm = mapIMP.get(cmpn_id);
				String record = getString(hm);
				String imp_k = getKey(record);
				String imp_v = getValue(record);
				ReducerKey.set(imp_k);
				ReducerVal.set(imp_v);
				context.write(ReducerKey, ReducerVal);
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Sequence Impressions and Conversions Completed.");
	}
}