package com.sharethis.socialopt.rt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.sharethis.socialopt.common.Constants;
import com.sharethis.socialopt.common.Utils;

public class SequenceDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text MapperKey = new Text();
	private Text MapperVal = new Text();
	private Pattern p_cmpn = Pattern.compile("\"cmpn\":\"(\\w+?)\"");
	private Pattern p_id = Pattern.compile("\"stid\":\"(\\.+?)\"");
	private HashSet<String> hs_ids;
	private HashSet<String> hs_cmpns;
	private HashSet<String> hs_moats;
	
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_RT_LOGGER_NAME);

	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Sequence Impressions and Conversions Mapper Starts.");
		
		hs_ids = new HashSet<String>();
		hs_cmpns = new HashSet<String>();
		hs_moats = new HashSet<String>();
		
		Configuration conf = context.getConfiguration();
		String str_pixels = conf.get("pixels");
		if (str_pixels != null && (!str_pixels.isEmpty())) {
			StringTokenizer st = new StringTokenizer(str_pixels, ",");
			while (st.hasMoreTokens()) {
				String[] tokens = st.nextToken().split("\\|");
				if (tokens.length >= 3) {
					if (tokens[2].equals("1") || tokens[2].equals("7")) {
						if (!hs_ids.contains(tokens[1]))
							hs_ids.add(tokens[1]);
						if (!hs_cmpns.contains(tokens[0]))
							hs_cmpns.add(tokens[0]);
					}
					else if (tokens[2].equals("8")) {
						if (!hs_moats.contains(tokens[1]))
							hs_moats.add(tokens[1]);
					}
				}
			}
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Input stream are a mixture of campaign hourly data and retargeting data
		String[] items = value.toString().split("\t");
		int n = items.length;
		if (n > 1) {
			String first = items[0];
			
			// If its a retargeting record
			if (first.length() == 32) {
				List<String> val = new ArrayList<String>();		
				List<String> moat = new ArrayList<String>();
				for (int i = 1; i < n; i++) {
					Matcher m_cmpn = p_cmpn.matcher(items[i]);
					Matcher m_id = p_id.matcher(items[i]);
					if (m_cmpn.find() && m_cmpn.groupCount() > 0 && hs_cmpns.contains(m_cmpn.group(1)))
							val.add(items[i]);
					else if (m_id.find() && m_id.groupCount() > 0 && m_id.group(1) == "9999")
						moat.add(items[i]);
				}
				if (val.size() > 0) {
					MapperKey.set(first);
					MapperVal.set("CONV" + "\t" + Joiner.on("\t").join(val));
					context.write(MapperKey, MapperVal);
				}
				if (moat.size() > 0) {
					MapperKey.set(first);
					MapperVal.set("MOAT" + "\t" + Joiner.on("\t").join(val));
					context.write(MapperKey, MapperVal);
				}
			}
			// Else it is a campaign hourly data format
			else {
				// Read input data stream
				HashMap<String, String> hm = new HashMap<String, String>();
				int limits = Constants.FIELDS.length - 6;
				if (items.length == Constants.FIELDS.length - 2)
					limits = Constants.FIELDS.length - 2;
				for (int i = 0; i < n && i < limits; i++) {
					if (items[i].isEmpty() || items[i].equalsIgnoreCase("unknown") || items[i].equalsIgnoreCase("null")) 
						items[i] = "-";
					hm.put(Constants.FIELDS[i], items[i]);
				}
				
				if (hm.containsKey("campaign_id") && hm.containsKey("cookie")) { 
					String cookie_id = hm.get("cookie");
					String conv_enable = "0";
					if (hs_ids.contains(hm.get("campaign_id")))
						conv_enable = "1";
					String moat_enable = "0";
					if (hs_moats.contains(hm.get("campaign_id")))
						moat_enable = "1";
					
					MapperKey.set(cookie_id);
					if (limits == Constants.FIELDS.length - 2)
						MapperVal.set("IMP" + "\t" + Utils.join("\t", items, 0, limits - 1));
					else
						MapperVal.set("IMP" + "\t" + Utils.join("\t", items, 0, limits - 1) + "\t" + conv_enable + "\t-\t" + moat_enable + "\t-");
					context.write(MapperKey, MapperVal);
				}
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Sequence Impressions and Conversions Mapper Completed.");
	}
}