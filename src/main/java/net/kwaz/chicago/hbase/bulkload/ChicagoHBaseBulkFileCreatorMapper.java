package net.kwaz.chicago.hbase.bulkload;

import net.kwaz.chicago.ChicagoKey;
import net.kwaz.chicago.ChicagoValue;
import net.kwaz.chicago.hbase.ChicagoHbaseKey;
import net.kwaz.chicago.hbase.ChicagoTableColumns;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ChicagoHBaseBulkFileCreatorMapper extends Mapper<ChicagoKey, ChicagoValue, ImmutableBytesWritable, Put> {
	
	private boolean prefixHash = false;
	
	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		prefixHash = Boolean.parseBoolean(conf.get(ChicagoHBaseBulkFileCreator.HASH_PREFIX));
	}
	
	@Override
	public void map(ChicagoKey key, ChicagoValue value, Context context) throws IOException, InterruptedException {
		ChicagoHbaseKey hbaseKey = new ChicagoHbaseKey(key, prefixHash);
		ImmutableBytesWritable outKey = new ImmutableBytesWritable(hbaseKey.toBytes());
		
		Put put = hbaseKey.createPut();
		
		for (ChicagoTableColumns c : ChicagoTableColumns.values()) {
			c.addToPut(put, value);
		}
		
		context.write(outKey, put);
	}
	
	@Override
	public void cleanup(Context context) throws IOException {
		// Nada
	}
	
}
