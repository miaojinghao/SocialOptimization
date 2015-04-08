package com.sharethis.socialopt.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.sharethis.socialopt.common.Constants;
import com.sharethis.socialopt.common.STDateUtils;

public class CollectDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text MapperKey = new Text();
	private Text MapperVal = new Text();
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_DATASET_LOGGER_NAME);
	
	private String get_item(String input, String flag) {
		String[] items = input.split("\\|");
		List<String> temp = new ArrayList<String>();
		for (int i = 0; i < items.length; i++) {
			if (!items[i].isEmpty())
				temp.add(items[i]);
		}
		
		String str_id = "-";
		int n = temp.size();
		if (n <= 0) return "-";
		else {
			if (flag.equals("rand")) {
				Random rand = new Random();
				int rndIndex = rand.nextInt(n);
				if (rndIndex < n) {
					String[] str_temp = temp.get(rndIndex).split(",");
					if (str_temp.length == 2)
						str_id = str_temp[0];
				}
			}
			else if (flag.equals("max")) {
				double max = 0.0;
				for (String each_temp: temp) {
					String[] str_temp = each_temp.split(",");
					try {
						if (str_temp.length == 2 && max <= Double.parseDouble(str_temp[1])) {
							max = Double.parseDouble(str_temp[1]);
							str_id = str_temp[0];
						}
					} catch (Exception e) {
						logger.error("Invalid weight: " + e.toString());
					}
				}
			}
		}
		if (str_id.isEmpty())
			str_id = "-";
		return str_id;
	}
	
	private String get_feature(int id, String[] tokens, int n) {
		if (id < n)
			return tokens[id];
		else
			return "-";
	}

	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Data collection mapper is starting.");
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] items= value.toString().split("\t");
		int n = items.length;
		String fea_vis = get_feature(3, items, n);
		String fea_usg = get_item(get_feature(7, items, n), "rand");
		String fea_vert = get_item(get_feature(8, items, n), "max");
		String fea_os = get_feature(37, items, n);
		String ts = get_feature(10, items, n);
		String fea_hour = STDateUtils.getHour(ts);
		String fea_day = STDateUtils.getDayOfWeek(ts);
		
		if (n - 2 >= 0) {
			String val = fea_usg + "\t" + fea_vert + "\t" + fea_os + "\t" + fea_hour + "\t" + fea_day;
			String conv_enable = items[n - 2];
			if (conv_enable.equals("1")) {
				String conv = items[n - 1];
				if (conv.equals("-")) {
					Random rn = new Random();
					int rand = rn.nextInt(50);
					if (rand == 0) {
						MapperKey.set(fea_vis);
						MapperVal.set(val + "\t0");
						context.write(MapperKey, MapperVal);
					}
				}
				else {
					MapperKey.set(fea_vis);
					MapperVal.set(val + "\t1");
					context.write(MapperKey, MapperVal);
				}	
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Data collection mapper is starting completed.");
	}
}