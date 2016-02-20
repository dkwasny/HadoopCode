package net.kwaz.chicago.pig;

import net.kwaz.HadoopUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class GeneratePigInput extends Configured implements Tool {
	
	@Parameter(names="-i", description="Input Path (Sequence File)", required=true)
	private String inputPathArg = "";
	
	@Parameter(names="-o", description="Output Path", required=true)
	private String outputOutputPathArg = "";
	
	@Parameter(names="-h", help=true)
	private boolean help = false;
	
	public static void main(String[] args) throws Exception {
		System.exit(
			ToolRunner.run(
				new GeneratePigInput(),
				args
			)
		);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		JCommander commander = new JCommander(this, arg0);
		if (help) {
			commander.usage();
			return 0;
		}
		
		Path inputPath = new Path(inputPathArg);
		Path outputPath = new Path(outputOutputPathArg);
		
		Job job = Job.getInstance(getConf(), "Generate Pig Input - " + inputPath + " -> " + outputPath);
		job.setJarByClass(getClass());
		
		job.getConfiguration().set("mapreduce.job.queuename", "kwaz-queue");
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);
		
		job.setMapperClass(GeneratePigInputMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputPath);

		HadoopUtils.addLibsToClasspath(job);

		return job.waitForCompletion(true) ? 0 : 1;
	}


	
}
