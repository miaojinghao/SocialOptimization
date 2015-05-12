package com.sharethis.socialopt.moat;

import java.io.IOException;
import java.util.HashMap;
// import java.util.regex.Matcher;
// import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.sharethis.socialopt.common.Constants;

public class MoatSequenceMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text MapperKey = new Text();
	private Text MapperVal = new Text();
	// private Pattern p_id = Pattern.compile("\"stid\":\"(\\.+?)\"");
	// private Pattern p_jid = Pattern.compile("\"jid\":\"(\\.+?)\"");
	
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_MOAT_LOGGER_NAME);

	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Moat Sequence Mapper Starts.");
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Input stream are a mixture of campaign hourly data and moat data
		String[] items = value.toString().split("\t");
		int n = items.length;
		if (n > 1) {
			String first = items[0];
			
			// If its a retargeting record
			if (first.length() == 32) {
				for (int i = 1; i < n; i++) {
					JSONObject obj = new JSONObject(items[i]);
					if (obj.has("stid") && obj.has("jid") && obj.get("stid").toString().equals("8888")) {
						String jid = obj.get("jid").toString();
						if (!jid.isEmpty()) {
							MapperKey.set(jid);
							MapperVal.set("MOAT" + "\t" + items[i]);
							context.write(MapperKey, MapperVal);
						}
					}
				}
			}
			// Else it is a campaign hourly data format
			else {
				// Read input data stream
				HashMap<String, String> hm = new HashMap<String, String>();
				for (int i = 0; i < n && i < Constants.FIELDS.length; i++) {
					if (items[i].isEmpty() || items[i].equalsIgnoreCase("unknown") || items[i].equalsIgnoreCase("null"))
						items[i] = "-";
					hm.put(Constants.FIELDS[i], items[i]);
				}
				
				if (hm.containsKey("jid")) {
					String jid = hm.get("jid");
					MapperKey.set(jid);
					MapperVal.set("IMP" + "\t" + value.toString());
					context.write(MapperKey, MapperVal);
				}
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Moat Sequence Mapper Completed.");
	}
}