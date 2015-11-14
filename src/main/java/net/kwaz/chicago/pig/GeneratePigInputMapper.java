package net.kwaz.chicago.pig;

import java.io.IOException;
import java.util.List;

import net.kwaz.chicago.ChicagoKey;
import net.kwaz.chicago.ChicagoValue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class GeneratePigInputMapper extends Mapper<ChicagoKey, ChicagoValue, Text, NullWritable> {
	
	private static final char SEP = ',';
	private static final char EVENT_SEP = '|';
	private static final DateTimeFormatter FORMAT = ISODateTimeFormat.dateTimeNoMillis();
	
	private final Text text = new Text();
	
	@Override
	public void map(ChicagoKey key, ChicagoValue value, Context context)
	throws IOException, InterruptedException
	{
		List<Object> items = ImmutableList.<Object>of(
			key.getZipCode(),
			FORMAT.print(key.getDateTimeUtc()),
			value.getTemperatureFahrenheit(),
			value.getDewPointFahrenheit(),
			value.getHumidity(),
			value.getSeaLevelInchesOfMercury(),
			value.getVisibilityMiles(),
			value.getWindDirection(),
			value.getWindSpeedMph(),
			value.getGustSpeedMph(),
			value.getPrecipitationInches(),
			Joiner.on(EVENT_SEP).join(value.getEvents()),
			value.getConditions(),
			value.getWindDirectionDegrees()
		);
		
		String output = Joiner.on(SEP).join(items);
		
		text.set(output);
		context.write(text, NullWritable.get());
	}
}
