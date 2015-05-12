package com.sharethis.socialopt.rt;

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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.sharethis.socialopt.common.ReadConf;
import com.sharethis.socialopt.common.Constants;
import com.sharethis.socialopt.common.STDateUtils;
// import com.sharethis.socialopt.rt.SequenceDataInputFormat;

public class SequenceDataRunner extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_RT_LOGGER_NAME);
	CommandLine cmd = null;
	
	int process_CLI(String[] args) {
		Options options = new Options();
		
		options.addOption("c", "pconf", true, "Process conf file");
		options.addOption("l", "lconf", true, "Logger conf file");
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
		// Process config file
		String str_config = "";
		if (cmd.hasOption('c') || cmd.hasOption("pconf")) {
			if (cmd.hasOption('c'))
				str_config = cmd.getOptionValue('c');
			else if (cmd.hasOption("pconf"))
				str_config = cmd.getOptionValue("pconf");
		}
		
		// Logger config file
		String str_log = "";
		if (cmd.hasOption('l') || cmd.hasOption("lconf")) {
			if (cmd.hasOption('l'))
				str_log = cmd.getOptionValue('l');
			else if (cmd.hasOption("lconf"))
				str_log = cmd.getOptionValue("lconf");
		}
		
		if (str_config.isEmpty() || str_log.isEmpty()) {
			logger.error("Config file(s) are missing.");
			return -1;
		}
		
		ReadConf rf = new ReadConf(str_config);
		logger_init(str_log);
		
		// Set output to be compressed
		conf.set("mapreduce.output.fileoutputformat.compress", "true");
		conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
		conf.set("pixels", rf.get("Pixels", ""));
		Job job = Job.getInstance(conf, "Impression Click and RT Sequence Data");
		job.setJarByClass(Class.forName(this.getClass().getName()));
		
		// Set Mapper
		job.setMapperClass(com.sharethis.socialopt.rt.SequenceDataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
				
		// Set Reducer
		job.setReducerClass(com.sharethis.socialopt.rt.SequenceDataReducer.class);
		job.setNumReduceTasks(Constants.NUM_REDUCER);
				
		// Set input and output key/value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Set input and output file format
		job.setInputFormatClass(TextInputFormat.class);
		// job.setInputFormatClass(SequenceDataInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		// Set input file paths
		int nDays = 0;
		try {
			nDays = Integer.parseInt(rf.get("Days", "1"));
			logger.info("Getting number of conversion data days: " + String.valueOf(nDays));
		} catch (NullPointerException e) {
			logger.error("Null input number of days." + e.toString());
			return -1;
		} catch (NumberFormatException e) {
			logger.error("Invalid input number of days. " + e.toString());
			return -1;
		}
		
		String str_startdate = rf.get("StartDate", "");
		if (!str_startdate.equals(""))
			logger.info("Getting start date: " + str_startdate);
		else {
			logger.error("Invalid input file start date.");
			return -1;
		}
		
		String str_inputdate = str_startdate;
		int nInputDays = nDays;
		if (nInputDays > 0) {
			for (int i = 0; i < 24; i++) {
				String hour = String.format("%02d", i);
				// String fileName = "s3n://sharethis-insights-backup/camp_data_hourly/" + str_startdate + hour + "/all_data_hourly";
				String fileName = "/user/btdev/projects/modeldata/prod/all_soc_hourly/" + str_inputdate + hour + "/all_soc_data_hourly";
				logger.info("Getting input files: " + fileName);
				Path p = new Path(fileName);
				FileSystem fs = p.getFileSystem(conf);
				if (fs.exists(p)) 
					FileInputFormat.addInputPath(job, p);
			}
			nInputDays--;
			str_inputdate = STDateUtils.getPreviousDay(str_inputdate);
		}
		while (nInputDays > 0) {
			// String fileName = "s3n://sharethis-insights-backup/camp_data_hourly/" + str_startdate + hour + "/all_data_hourly";
			String fileName = "/user/btdev/projects/modeldata/prod/rt_daily/" + str_inputdate;
			Path p = new Path(fileName);
			FileSystem fs = p.getFileSystem(conf);
			if (fs.exists(p)) {
				logger.info("Getting input files: " + fileName);
				FileInputFormat.addInputPath(job, p);
			}
			else {
				for (int i = 0; i < 24; i++) {
					String hour = String.format("%02d", i);
					// String fileName = "s3n://sharethis-insights-backup/camp_data_hourly/" + str_startdate + hour + "/all_data_hourly";
					fileName = "/user/btdev/projects/modeldata/prod/all_soc_hourly/" + str_inputdate + hour + "/all_soc_data_hourly";
					logger.info("Getting input files: " + fileName);
					p = new Path(fileName);
					fs = p.getFileSystem(conf);
					if (fs.exists(p)) 
						FileInputFormat.addInputPath(job, p);
				}
			}
			nInputDays--;
			str_inputdate = STDateUtils.getPreviousDay(str_inputdate);
		}
				
		// while (nDays > 0) {
		for (int i = 0; i < 24; i++) {
			String hour = String.format("%02d", i);
			String fileName = "s3n://sharethis-campaign-analytics/retarg/" + str_startdate + hour + "/data";
			// String fileName = "s3n://sharethis-research/campaign_analytics/retarg/" + str_startdate + hour + "/data";
			logger.info("Getting input file: " + fileName);
			Path p = new Path(fileName);
			FileSystem fs = p.getFileSystem(conf);
			if (fs.exists(p)) 
				FileInputFormat.addInputPath(job, p);
		}
		//	nDays--;
		//	str_startdate = STDateUtils.getNextDay(str_startdate);
		// }
		
		// Set output file path
		String output_path = rf.get("OutputPath", "");
		if (output_path != null && !output_path.isEmpty()) {
			logger.info("Getting output path: " + output_path + "/temp");
			Path p = new Path(output_path + "/temp");
			FileSystem fs = p.getFileSystem(conf);
			if (fs.exists(p))
				fs.delete(p, true);
			FileOutputFormat.setOutputPath(job, p);
		}
		else {
			logger.error("Output path is not specified.");
			return -1;
		}
		
		boolean ret = job.waitForCompletion(true);
		if (ret) {
			// Move output file
			int nOutputDays = nDays;
			String str_outputdate = str_startdate;
			while (nOutputDays > 0) {
				Path src = new Path(output_path + "/temp/" + str_outputdate);
				Path dst = new Path(output_path + "/" + str_outputdate);
				FileSystem fs = src.getFileSystem(conf);
				if (fs.exists(src)) {
					if (fs.exists(dst))
						fs.delete(dst, true);
					fs.rename(src, dst);
				}
				nOutputDays--;
				str_outputdate = STDateUtils.getPreviousDay(str_outputdate);
			}
			return 1;
		}
		else
			return 0;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		SequenceDataRunner runner = new SequenceDataRunner();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = ToolRunner.run(conf, runner, args);
		System.exit(exitCode);
	}
}