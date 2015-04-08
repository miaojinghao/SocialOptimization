package com.sharethis.socialopt.rt;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.sharethis.socialopt.common.Constants;
import com.sharethis.socialopt.common.Utils;

public class SequenceDataInputFormat extends FileInputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new MyRecordReader();
	}
	
	class MyRecordReader extends RecordReader<Text, Text> {
		private LineRecordReader lineReader;
		private Text key = new Text();
		private Text value = new Text();

		public void close() throws IOException {
			lineReader.close();
		}

		public float getProgress() throws IOException {
			return lineReader.getProgress();
		}


		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
			lineReader = new LineRecordReader();
			lineReader.initialize(genericSplit, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!lineReader.nextKeyValue())
				return false;
			
			String items[] = lineReader.getCurrentValue().toString().split("\t");
			int n = items.length;
			if (n > 0) {
				// RT data
				if (items[0].length() == 32) {
					key.set(items[0]);
					value.set("CONV\t" + Utils.join("\t", items, 1, n - 1));
				}
				else {
					HashMap<String, String> hm = new HashMap<String, String>();
			
					for (int i = 0; i < n && i < Constants.FIELDS.length; i++) {
						String item = items[i];
						if (item.isEmpty() || item.equalsIgnoreCase("unknown") || item.equalsIgnoreCase("null")) {
							items[i] = "-";
							item = "-";
						}
						hm.put(Constants.FIELDS[i], item);
					}
			
					String cookie = "-";
					if (hm.containsKey("cookie"))
						cookie = hm.get("cookie");
					key.set(cookie);
					value.set("IMP\t" + value.toString());
				}
			}
			return true;
		}
	}
}