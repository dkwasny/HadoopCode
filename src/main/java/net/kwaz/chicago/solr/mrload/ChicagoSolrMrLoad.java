package net.kwaz.chicago.solr.mrload;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class ChicagoSolrMrLoad extends Configured implements Tool {
	
	private static final Logger LOG = LoggerFactory.getLogger(ChicagoSolrMrLoad.class);

	public static void main(String[] args)
	throws Exception
	{
		ToolRunner.run(new ChicagoSolrMrLoad(), args);
	}

	@Override
	public int run(String[] args)
	throws Exception
	{
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		File solrHomeDir = new File(args[2]);
		if (!solrHomeDir.exists()) {
			throw new IllegalArgumentException(
					"Provided solr home does not exist");
		}
		String zkQuorum = args[3];
		String collectionName = args[4];
		
		LOG.info("Getting shard data");
		MyGoLive goLive = new MyGoLive(zkQuorum, collectionName);
		Collection<ShardData> shardDatas = goLive.getShardDatas();

		LOG.info("Creating job");
		Job job = getJob(
			inputPath,
			outputPath,
			solrHomeDir,
			shardDatas.size()
		);
		
		LOG.info("Running job");
		if (!job.waitForCompletion(true)) {
			System.out.println("Job failed");
			return 1;
		}
		
		LOG.info("Retrieving index paths");
		Collection<Path> indexPaths = getOutputIndexPaths(job);
		
		LOG.info("Merging indexes");
		goLive.mergeIndexes(collectionName, shardDatas, indexPaths);
		
		LOG.info("Deleting unnecessary artifacts");
		FileSystem fs = null;
		try {
			fs = FileSystem.get(getConf());
			fs.delete(outputPath, true);
		}
		finally {
			if (fs != null) { fs.close(); }			
		}
		
		LOG.info("Finished!");
		return 0;
	}
	
	private Collection<Path> getOutputIndexPaths(Job job)
	throws IOException
	{
		Path basePath = SolrOutputFormat.getOutputPath(job);
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] statuses = fs.globStatus(basePath.suffix("/*/data/index"));
		
		List<Path> retVal = Lists.newArrayList();
		for (FileStatus status : statuses) {
			retVal.add(status.getPath());
		}
		return retVal;
	}
	
	private Job getJob(
		Path inputPath,
		Path outputPath,
		File solrHomeDir,
		int numReducers)
	throws IOException
	{
		Job job = Job.getInstance(getConf(), "Solr MR Load");
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputPaths(job, inputPath);

		job.setMapperClass(ChicagoSolrMrLoadMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SolrInputDocumentWritable.class);

		job.setReducerClass(SolrReducer.class);
		job.setNumReduceTasks(numReducers);

		job.setOutputFormatClass(SolrOutputFormat.class);
		
		// Those solr fools have a class that will download the zookeeper
		// configs for me, but the damn thing is package private.
		// I will just assume the configs are saved locally for now.
		// -Kwaz
		SolrOutputFormat.setupSolrHomeCache(solrHomeDir, job);
		SolrOutputFormat.setOutputPath(job, outputPath);
		
		return job;
	}

}
