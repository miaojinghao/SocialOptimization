package com.sharethis.socialopt.moat;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.sharethis.socialopt.common.Constants;
import com.sharethis.socialopt.common.STDateUtils;

public class MoatSequenceRunner extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_MOAT_LOGGER_NAME);
	CommandLine cmd = null;
	
	int process_CLI(String[] args) {
		Options options = new Options();
		
		options.addOption("d", "date", true, "Date of files to be processed");
		options.addOption("l", "lconf", true, "Logger conf file");
		options.addOption("n", "ndays", true, "Number of days of MOAT data");
		options.addOption("o", "output", true, "Output file path");
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
	
	protected void logger_init(String conf_file) {
		PropertyConfigurator.configure(conf_file);
		logger.setLevel(Level.INFO);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		if (process_CLI(args) == -1)
			return -1;
		
		// Command line parameters
		// Process input date
		String str_date = "";
		if (cmd.hasOption('d') || cmd.hasOption("date")) {
			if (cmd.hasOption('d'))
				str_date = cmd.getOptionValue('d');
			else if (cmd.hasOption("date"))
				str_date = cmd.getOptionValue("date");
		}
		if (str_date.isEmpty()) {
			logger.error("Invalid date: " + str_date);
			return -1;
		}
		else
			logger.info("Getting date: " + str_date);
		
		// Logger config file
		String str_log = "";
		if (cmd.hasOption('l') || cmd.hasOption("lconf")) {
			if (cmd.hasOption('l'))
				str_log = cmd.getOptionValue('l');
			else if (cmd.hasOption("lconf"))
				str_log = cmd.getOptionValue("lconf");
		}
		if (str_log.isEmpty()) {
			logger.error("Invalid log config file: " + str_log);
			return -1;
		}
		else
			logger.info("Getting log config file: " + str_log);
		
		// Number of days
		int nDays = 0;
		String str_ndays = "";
		if (cmd.hasOption('n') || cmd.hasOption("ndays")) {
			if (cmd.hasOption('n'))
				str_ndays = cmd.getOptionValue('n');
			else if (cmd.hasOption("ndays"))
				str_ndays = cmd.getOptionValue("ndays");
		}
		try {
			nDays = Integer.parseInt(str_ndays);
			logger.info("Getting number of conversion data days: " + String.valueOf(nDays));
		} catch (NullPointerException e) {
			logger.error("Null input number of days." + e.toString());
			return -1;
		} catch (NumberFormatException e) {
			logger.error("Invalid input number of days. " + e.toString());
			return -1;
		}
		
		// Output path
		String str_output = "";
		if (cmd.hasOption('o') || cmd.hasOption("output")) {
			if (cmd.hasOption('o'))
				str_output = cmd.getOptionValue('o');
			else if (cmd.hasOption("output"))
				str_output = cmd.getOptionValue("output");
		}
		if (str_output.isEmpty()) {
			logger.error("Invalid output file path: " + str_output);
			return -1;
		}
		
		// Log file configuration
		logger_init(str_log);
		
		// Set output to be compressed
		conf.set("mapreduce.output.fileoutputformat.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
		Job job = Job.getInstance(conf, "MOAT data integration");
		job.setJarByClass(Class.forName(this.getClass().getName()));
		
		// Set Mapper
		job.setMapperClass(com.sharethis.socialopt.moat.MoatSequenceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
				
		// Set Reducer
		job.setReducerClass(com.sharethis.socialopt.moat.MoatSequenceReducer.class);
		job.setNumReduceTasks(Constants.NUM_REDUCER);
				
		// Set input and output key/value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Set input and output file format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set mapred input file path
		String fileName = "s3n://sharethis-insights-backup/rt_daily/" + str_date;
		logger.info("Getting input file: " + fileName);
		Path p = new Path(fileName);
		FileSystem fs = p.getFileSystem(conf);
		if (fs.exists(p)) 
			FileInputFormat.addInputPath(job, p);
		else {
			logger.error("Missing input file path: " + fileName);
			return -1;
		}
		
		while (nDays > 0) {
			for (int i = 0; i < 24; i++) {
				String hour = String.format("%02d", i);
				fileName = "s3n://sharethis-campaign-analytics/retarg/" + str_date + hour + "/data";
				// fileName = "s3n://sharethis-research/campaign_analytics/retarg/" + str_date + hour + "/data";
				logger.info("Getting input file: " + fileName);
				p = new Path(fileName);
				fs = p.getFileSystem(conf);
				if (fs.exists(p)) 
					FileInputFormat.addInputPath(job, p);
			}
			nDays--;
			str_date = STDateUtils.getNextDay(str_date);
		}
		
		// Set output file path
		logger.info("Getting output path: " + str_output);
		p = new Path(str_output);
		fs = p.getFileSystem(conf);
		if (fs.exists(p))
			fs.delete(p, true);
		FileOutputFormat.setOutputPath(job, p);
	
		return job.waitForCompletion(true) ? 1 : 0;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		MoatSequenceRunner runner = new MoatSequenceRunner();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = ToolRunner.run(conf, runner, args);
		System.exit(exitCode);
	}
}