package net.kwaz.chicago.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ChicagoAnalyzerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void setup(Context context) {
		//Nothing
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int finalVal = 0;
		for (IntWritable value : values) {
			finalVal += value.get();
		}
		context.write(key, new IntWritable(finalVal));
	}
	
	@Override
	public void cleanup(Context context) {
		//Nothing
	}
	
}