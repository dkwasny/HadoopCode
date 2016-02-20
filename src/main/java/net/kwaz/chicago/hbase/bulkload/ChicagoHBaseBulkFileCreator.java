package net.kwaz.chicago.hbase.bulkload;

import net.kwaz.HadoopUtils;
import net.kwaz.chicago.parser.ChicagoRawDataParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class ChicagoHBaseBulkFileCreator extends Configured implements Tool {
	
	public static final String HASH_PREFIX = "kwaz.hash-prefix";
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ChicagoHBaseBulkFileCreator(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		TableName outputTable = TableName.valueOf(args[2]);
		
		Configuration conf = getConf();
		conf.addResource("hbase-site.xml");
		
		conf.set("mapreduce.job.queuename", "kwaz-queue");
		
		conf.set(HASH_PREFIX, Boolean.toString(true));
		
		Job job = Job.getInstance(conf, "ChicagoDataHBaseBulkFileCreator - " + inputPath + " -> " + outputPath + " -> " + outputTable);
		job.setJarByClass(ChicagoRawDataParser.class);
		
		job.setMapperClass(ChicagoHBaseBulkFileCreatorMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, inputPath);
		SequenceFileInputFormat.setMaxInputSplitSize(job, Integer.MAX_VALUE);
		
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		HTable table = new HTable(conf, outputTable);
		HFileOutputFormat2.configureIncrementalLoad(job, table);
		HFileOutputFormat2.setOutputPath(job, outputPath);

		HadoopUtils.addLibsToClasspath(job);

		int retVal = 0;
		if (job.waitForCompletion(true)) {
			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
			loader.doBulkLoad(outputPath, table);
		}
		else {
			System.out.println("Bulkload failed!");
			retVal = 1;
		}

		return retVal;
	}
	
}
