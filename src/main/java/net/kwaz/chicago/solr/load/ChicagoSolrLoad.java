package net.kwaz.chicago.solr.load;

import java.io.IOException;

import net.kwaz.HadoopUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChicagoSolrLoad extends Configured implements Tool {

	public static final String POST_URL = "kwaz.post.url";
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ChicagoSolrLoad(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		String postUrl = args[1];
		
		Job job = createJob(inputPath, postUrl);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private Job createJob(Path inputPath, String postUrl) throws IOException {
		String jobName = "ChicagoSolrLoad - " + inputPath.toUri().toString() + " -> " + postUrl;
		Job job = Job.getInstance(getConf(), jobName);
		
		job.getConfiguration().set(POST_URL, postUrl);
		
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, inputPath);
		
		job.setMapperClass(ChicagoSolrLoadMapper.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputFormatClass(NullOutputFormat.class);

		HadoopUtils.addLibsToClasspath(job);
		
		return job;
	}
}
