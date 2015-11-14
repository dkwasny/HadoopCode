package net.kwaz.chicago.hbase.table;

import net.kwaz.chicago.hbase.ChicagoTableColumns;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChicagoTableCreator extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ChicagoTableCreator(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		String stringTableName = args[0];
		
		Configuration config = getConf();
		config.addResource("hbase-site.xml");
		
		HBaseAdmin admin = new HBaseAdmin(config);
		
		TableName tableName = TableName.valueOf(stringTableName);
		
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(ChicagoTableColumns.CF);
		columnDescriptor.setCompressionType(Compression.Algorithm.GZ);
		columnDescriptor.setCompactionCompressionType(Compression.Algorithm.GZ);
		
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		tableDescriptor.addFamily(columnDescriptor);
		tableDescriptor.setMaxFileSize(1000 * 1000 * 1024);
		tableDescriptor.setConfiguration(HTableDescriptor.SPLIT_POLICY, "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy");
		
		RegionSplitter.UniformSplit split = new RegionSplitter.UniformSplit();
		admin.createTable(tableDescriptor, split.split(4));
		
		admin.close();
		
		return 0;
	}
	
}
