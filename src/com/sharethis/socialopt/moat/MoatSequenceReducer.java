package com.sharethis.socialopt.moat;

import java.io.IOException;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.sharethis.socialopt.common.Constants;
import com.sharethis.socialopt.common.STDateUtils;
import com.sharethis.socialopt.common.Utils;

public class MoatSequenceReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_MOAT_LOGGER_NAME);	
	private Pattern p_date = Pattern.compile("\"date\":\"(\\.+?)\"");
	
	private Text ReducerKey = new Text();
	private Text ReducerVal = new Text();
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Moat Sequence Reducer Starts.");
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
		return Utils.join("\t", tokens, 1, n - 1);
	}
	
	public String getObjString(String s) {
		JSONObject obj = new JSONObject(s);
		JSONObject target = new JSONObject();
		if (obj.has("d") && !obj.isNull("d"))
			target.put("d", obj.get("d"));
		if (obj.has("ivm") && !obj.isNull("ivm"))
			target.put("ivm", obj.get("ivm"));
		if (obj.has("ivt") && !obj.isNull("ivt"))
			target.put("ivt", obj.get("ivt"));
		if (obj.has("ivi") && !obj.isNull("ivi"))
			target.put("ivi", obj.get("ivi"));
		if (obj.has("ui") && !obj.isNull("ui"))
			target.put("ui", obj.get("ui"));
		if (obj.has("uit") && !obj.isNull("uit"))
			target.put("uit", obj.get("uit"));
		if (obj.has("hover") && !obj.isNull("hover"))
			target.put("hover", obj.get("hover"));
		if (obj.has("tuh") && !obj.isNull("tuh"))
			target.put("tuh", obj.get("tuh"));
		if (obj.has("adgid") && !obj.isNull("adgid"))
			target.put("adgid", obj.get("adgid"));
		if (obj.has("dmn") && !obj.isNull("dmn"))
			target.put("dmn", obj.get("dmn"));
		if (obj.has("cpid") && !obj.isNull("cpid"))
			target.put("cpid", obj.get("cpid"));
		if (obj.has("crtvid") && !obj.isNull("crtvid"))
			target.put("crtvid", obj.get("crtvid"));
		if (obj.has("jid") && !obj.isNull("jid"))
			target.put("jid", obj.get("jid"));
		return target.toString();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		TreeMap<String, String> tmMoat = new TreeMap<String, String>();
		TreeMap<String, String> tmImp = new TreeMap<String, String>();
		
		for (Text value: values) {
			String[] items = value.toString().split("\t");
			int n = items.length;
			if (n <= 1)
				continue;
			else if (items[0].equals("MOAT")) {
				String date = get_pattern(items[1], p_date);
				String engagement = getObjString(items[1]);
				tmMoat.put(date, engagement);
			}
			else if (items[0].equals("IMP")) {
				String date = STDateUtils.format(items[11]);
				tmImp.put(date, Utils.join("\t", items, 1, n - 1) + "\t0\t-");
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
					else if (obj.has("jid") && obj.get("jid").toString().equals(items[12])) 
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
			context.write(ReducerKey, ReducerVal);
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Moat Sequence Reducer Completed.");
	}
}