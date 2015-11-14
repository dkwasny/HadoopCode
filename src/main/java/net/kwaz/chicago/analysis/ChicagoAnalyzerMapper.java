package net.kwaz.chicago.analysis;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

public class ChicagoAnalyzerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static enum Counter {
		SKIPPED_LINES;
	}
	
	private static enum DataType {
		TEMPERATURE_F,
		DEW_POINT_F,
		HUMIDITY,
		SEA_LEVEL,
		VISIBILITY,
		WIND_DIRECTION,
		WIND_SPEED,
		GUST_SPEED,
		PRECIPITATION,
		EVENTS,
		CONDITIONS,
		WIND_DIRECTION_DEGREES;
		
		private Text getText(String suffix) {
			return new Text(this.name() + '-' + suffix);
		}
	}
	
	private static final IntWritable ONE = new IntWritable(1);
	
	@Override
	public void setup(Context context) {
		//Nothing
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String valString = value.toString();
		
		// Skip "header" lines
		if (Strings.isNullOrEmpty(valString) || valString.startsWith("Time") || valString.startsWith("No daily")) {
			context.getCounter(Counter.SKIPPED_LINES).increment(1l);
			return;
		}
		
		//sanitize inputs
		String sanitizedValue = valString.replace("<br />", "");
		Iterator<String> data = Splitter.on(',').split(sanitizedValue).iterator();
		
		// Skip first value (EST time)
		data.next();
		
		parseFloat(DataType.TEMPERATURE_F, data.next(), context);
		parseFloat(DataType.DEW_POINT_F, data.next(), context);
		parseInt(DataType.HUMIDITY, data.next(), context);
		parseFloat(DataType.SEA_LEVEL, data.next(), context);
		parseFloat(DataType.VISIBILITY, data.next(), context);
		context.write(DataType.WIND_DIRECTION.getText(data.next()), ONE);
		parseFloat(DataType.WIND_SPEED, data.next(), context);
		parseFloat(DataType.GUST_SPEED, data.next(), context);
		parseFloat(DataType.PRECIPITATION, data.next(), context);
		context.write(DataType.EVENTS.getText(data.next()), ONE);
		context.write(DataType.CONDITIONS.getText(data.next()), ONE);
		parseInt(DataType.WIND_DIRECTION_DEGREES, data.next(), context);
		
		// Skip the UTC time
		data.next();
		
		if (data.hasNext()) {
			StringBuilder builder = new StringBuilder();
			while (data.hasNext()) {
				builder.append(data.next());
			}
			throw new IllegalStateException("We are skipping data (" + builder.toString() + ")!");
		}
	}
	
	@Override
	public void cleanup(Context context) {
		//Nothing
	}
	
	private int parseInt(DataType datatype, String value, Context context) throws IOException, InterruptedException {
		int retVal = Integer.MIN_VALUE;
		
		try {
			retVal = Integer.parseInt(value);
			context.write(datatype.getText("Valid"), ONE);
		}
		catch (NumberFormatException nfe) {
			context.write(datatype.getText(value), ONE);
		}
		
		return retVal;
	}
	
	private float parseFloat(DataType datatype, String value, Context context) throws IOException, InterruptedException {
		float retVal = Float.NaN;
		
		try {
			retVal = Float.parseFloat(value);
			context.write(datatype.getText("Valid"), ONE);
		}
		catch (NumberFormatException nfe) {
			context.write(datatype.getText(value), ONE);
		}
		
		return retVal;
	}
	
}
