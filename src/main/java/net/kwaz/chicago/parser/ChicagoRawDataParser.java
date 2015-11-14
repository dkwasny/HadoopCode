package net.kwaz.chicago.parser;

import net.kwaz.chicago.ChicagoKey;
import net.kwaz.chicago.ChicagoValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ChicagoRawDataParser extends Configured implements Tool {
	
	@Parameter(names="-i", description="Input Path", required=true)
	private String inputPathArg = "";
	
	@Parameter(names="-o", description="Output Path", required=true)
	private String outputPathArg = "";
	
	@Parameter(names="-h", help=true)
	private boolean help = false;
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ChicagoRawDataParser(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		JCommander commander = new JCommander(this, args);
		if (help) {
			commander.usage();
			return 0;
		}
		
		Path inputPath = new Path(inputPathArg);
		Path outputPath = new Path(outputPathArg);
		
		Configuration conf = getConf();
		
		conf.set("mapreduce.job.queuename", "kwaz-queue");
		
		Job job = Job.getInstance(conf, "ChicagoWeatherRawDataParser - " + inputPath + " -> " + outputPath);
		job.setJarByClass(ChicagoRawDataParser.class);
		
		job.setMapperClass(ChicagoRawDataParserMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, inputPath);
		
		job.setMapOutputKeyClass(ChicagoKey.class);
		job.setMapOutputValueClass(ChicagoValue.class);
		
		job.setReducerClass(ChicagoRawDataParserReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(ChicagoKey.class);
		job.setOutputValueClass(ChicagoValue.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(4);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
