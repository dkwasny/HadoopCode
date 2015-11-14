package net.kwaz.chicago.analysis;

import net.kwaz.chicago.parser.ChicagoRawDataParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChicagoAnalyzer extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ChicagoAnalyzer(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Configuration conf = getConf();
		conf.set("mapreduce.job.queuename", "kwaz-queue");
		
		Job job = Job.getInstance(conf, "ChicagoWeatherRawDataAnalyzer - " + inputPath + " -> " + outputPath);
		job.setJarByClass(ChicagoRawDataParser.class);
		
		job.setMapperClass(ChicagoAnalyzerMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, inputPath);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(ChicagoAnalyzerReducer.class);
		
		job.setReducerClass(ChicagoAnalyzerReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		TextOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}
