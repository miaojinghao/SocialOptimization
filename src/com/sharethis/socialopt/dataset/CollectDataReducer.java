package com.sharethis.socialopt.dataset;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.sharethis.socialopt.common.Constants;

public class CollectDataReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_DATASET_LOGGER_NAME);	
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Data collection reducer is starting.");
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value: values)
			context.write(key, value);
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Data collection reducer completed.");
	}
}