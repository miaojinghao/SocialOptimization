package com.sharethis.socialopt.dataset;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.sharethis.socialopt.common.Constants;
import com.sharethis.socialopt.common.STDateUtils;

public class CollectDataRunner extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_DATASET_LOGGER_NAME);
	CommandLine cmd = null;
	
	private int process_CLI(String[] args) {
		Options options = new Options();
		
		options.addOption("i", "input", true, "Input file location");
		options.addOption("d", "enddate", true, "Input file end date");
		options.addOption("n", "days", true, "Number of days back from the end date");
		options.addOption("o", "output", true, "Output file location");
		CommandLineParser parser = new BasicParser();
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			logger.error("Command line parse error: " + e.toString());
			cmd = null;
			return -1;
		}
		return 0;
	}
	
	private String process_parameters(char c, String opt) {
		String s = "";
		if (cmd != null) {
			if (cmd.hasOption(c) || cmd.hasOption(opt)) {
				if (cmd.hasOption(c))
					s = cmd.getOptionValue(c);
				else if (cmd.hasOption(opt))
					s = cmd.getOptionValue(opt);
			}
		}
		return s;
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		if (process_CLI(args) == -1 || cmd == null)
			return -1;
		
		/*
		 * Process command line parameters
		 */
		String str_input = process_parameters('i', "input");
		String str_date = process_parameters('d', "enddate");
		String str_ndays = process_parameters('n', "days");
		String str_output = process_parameters('o', "output");
		
		if (str_input.isEmpty() || str_date.isEmpty() || str_ndays.isEmpty() || str_output.isEmpty()) {
			logger.error("Missing input parameter(s).");
			return -1;
		}
		else {
			str_output = str_output + "/" + str_date;
			logger.info("Getting input path: " + str_input);
			logger.info("Getting the end date: " + str_date);
			logger.info("Getting number of days: " + str_ndays);
			logger.info("Getting output path: " + str_output);
		}
		
		// Set output to be compressed
		conf.set("mapreduce.output.fileoutputformat.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
		Job job = Job.getInstance(conf, "Modeling Data Set");
		job.setJarByClass(Class.forName(this.getClass().getName()));
		
		// Set Mapper
		job.setMapperClass(com.sharethis.socialopt.dataset.CollectDataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
				
		// Set Reducer
		job.setReducerClass(com.sharethis.socialopt.dataset.CollectDataReducer.class);
		job.setNumReduceTasks(20);
				
		// Set input and output key/value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Set input and output file format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set input file paths
		int nDays = 0;
		try {
			nDays = Integer.parseInt(str_ndays);
		} catch (NullPointerException e) {
			logger.error("Null input number of days." + e.toString());
			nDays = 0;
			return -1;
		} catch (NumberFormatException e) {
			logger.error("Invalid input number of days. " + e.toString());
			nDays = 0;
			return -1;
		}
		
		while (nDays > 0) {
			String fileName = str_input + "/" + str_date;
			Path p = new Path(fileName);
			FileSystem fs = p.getFileSystem(conf);
			if (fs.exists(p)) 
				FileInputFormat.addInputPath(job, p);
			nDays--;
			str_date = STDateUtils.getPreviousDay(str_date);
		}
		
		// Set output file path
		Path p = new Path(str_output);
		FileSystem fs = p.getFileSystem(conf);
		if (fs.exists(p))
			fs.delete(p, true);
		FileOutputFormat.setOutputPath(job, p);
		
		return job.waitForCompletion(true) ? 1 : 0;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		CollectDataRunner runner = new CollectDataRunner();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = ToolRunner.run(conf, runner, args);
		System.exit(exitCode);
	}
}