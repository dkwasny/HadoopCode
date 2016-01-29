package net.kwaz.chicago.parser;

import net.kwaz.chicago.ChicagoKey;
import net.kwaz.chicago.ChicagoValue;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ChicagoRawDataParserReducer extends Reducer<ChicagoKey, ChicagoValue, ChicagoKey, ChicagoValue> {
	
	private enum Counter {
		DUPE_KEYS_SAME_VALUE,
		DUPE_KEYS_DIFF_VALUE
	}
	
	@Override
	public void setup(Context context) {
		//Nothing
	}
	
	@Override
	public void reduce(ChicagoKey key, Iterable<ChicagoValue> values, Context context) throws IOException, InterruptedException {
		Iterator<ChicagoValue> iter = values.iterator();
		ChicagoValue origValue = iter.next();
		context.write(key, origValue);
		while (iter.hasNext()) {
			ChicagoValue val = iter.next();
			if (origValue.equals(val)) {
				context.getCounter(Counter.DUPE_KEYS_SAME_VALUE).increment(1L);
			}
			else{
				context.getCounter(Counter.DUPE_KEYS_DIFF_VALUE).increment(1L);
			}
		}
	}
	
	@Override
	public void cleanup(Context context) {
		//Nothing
	}

}
