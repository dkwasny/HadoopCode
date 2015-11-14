package net.kwaz.chicago.parser;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import net.kwaz.chicago.ChicagoKey;
import net.kwaz.chicago.ChicagoValue;
import net.kwaz.chicago.Event;
import net.kwaz.chicago.WindDirection;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;

public class ChicagoRawDataParserMapper extends Mapper<LongWritable, Text, ChicagoKey, ChicagoValue> {
	
	private enum Counter {
		SKIPPED_LINES,
		EVENT_PARSES_GOOD,
		EVENT_PARSES_BAD,
		EVENT_PARSES_EMPTY,
		WIND_DIRECTION_PARSES_GOOD,
		WIND_DIRECTION_PARSES_BAD,
		NUMERIC_PARSES_GOOD,
		NUMERIC_PARSES_BAD;
	}
	
	@Override
	public void setup(Context context) {
		//Nothing
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String valString = value.toString();
		
		// Skip bogus lines
		if (Strings.isNullOrEmpty(valString) || valString.startsWith("Time") || valString.startsWith("No daily")) {
			context.getCounter(Counter.SKIPPED_LINES).increment(1l);
			return;
		}
		
		//ScaryCast
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		Path splitPath = fileSplit.getPath();
		int zipCode = Integer.parseInt(splitPath.getName());
		
		//sanitize inputs
		String sanitizedValue = valString.replace("<br />", "");
		Iterator<String> data = Splitter.on(',').split(sanitizedValue).iterator();
		
		// Skip first value (EST time)
		data.next();
		
		float temperatureFahrenheit = parseFloat(data.next(), context);
		float dewPointFahrenheit = parseFloat(data.next(), context);
		int humidity = parseInt(data.next(), context);
		float seaLevelInchesOfMercury = parseFloat(data.next(), context);
		float visibilityMiles = parseFloat(data.next(), context);
		WindDirection windDirection = parseWindDirection(data.next(), context);
		float windSpeedMph = parseWindSpeed(data.next(), context);
		float gustSpeedMph = parseFloat(data.next(), context);
		float precipitationInches = parseFloat(data.next(), context);
		EnumSet<Event> events = parseEvents(data.next(), context);
		String conditions = data.next();
		int windDirectionDegrees = parseInt(data.next(), context);
		
		String dateString = data.next().replace(" ", "T").concat("+0000");
		DateTime dateTimeUtc = ISODateTimeFormat.dateTimeNoMillis().parseDateTime(dateString);
		
		if (data.hasNext()) {
			StringBuilder builder = new StringBuilder();
			while (data.hasNext()) {
				builder.append(data.next());
			}
			throw new IllegalStateException("We are skipping data (" + builder.toString() + ")!");
		}
		
		ChicagoKey newKey = new ChicagoKey(zipCode, dateTimeUtc);
		ChicagoValue newValue = new ChicagoValue(
				temperatureFahrenheit,
				dewPointFahrenheit,
				humidity,
				seaLevelInchesOfMercury,
				visibilityMiles,
				windDirection,
				windSpeedMph,
				gustSpeedMph,
				precipitationInches,
				events,
				conditions,
				windDirectionDegrees
		);
		
		context.write(newKey, newValue);
	}
	
	@Override
	public void cleanup(Context context) {
		//Nothing
	}
	
	private WindDirection parseWindDirection(String raw, Context context) {
		WindDirection windDirection = WindDirection.fromSourceValue(raw);
		if (windDirection == WindDirection.UNKNOWN) {
			context.getCounter(Counter.WIND_DIRECTION_PARSES_BAD).increment(1l);
		}
		else {
			context.getCounter(Counter.WIND_DIRECTION_PARSES_GOOD).increment(1l);
		}
		return windDirection;
	}
	
	private EnumSet<Event> parseEvents(String raw, Context context) {
		EnumSet<Event> retVal = EnumSet.noneOf(Event.class);
		if (!Strings.isNullOrEmpty(raw)) {
			Iterable<String> values = Splitter.on('-').split(raw);
			for (String value : values) {
				Event event = Event.fromSourceValue(value);
				if (event == Event.UNKNOWN) {
					context.getCounter(Counter.EVENT_PARSES_BAD).increment(1l);
				}
				else {
					context.getCounter(Counter.EVENT_PARSES_GOOD).increment(1l);
				}
				retVal.add(event);
			}
		}
		else {
			context.getCounter(Counter.EVENT_PARSES_EMPTY).increment(1l);
		}
		return retVal;
	}
	
	private float parseWindSpeed(String value, Context context) {
		float retVal;
		
		if ("Calm".equals(value)) {
			retVal = 0f;
		}
		else {
			retVal = parseFloat(value, context);
		}
		
		return retVal;
	}
	
	private int parseInt(String value, Context context) {
		int retVal = ChicagoValue.INVALID_INT;
		
		try {
			retVal = Integer.parseInt(value);
			context.getCounter(Counter.NUMERIC_PARSES_GOOD).increment(1l);
		}
		catch (NumberFormatException nfe) {
			context.getCounter(Counter.NUMERIC_PARSES_BAD).increment(1l);
		}
		
		return retVal;
	}
	
	private float parseFloat(String value, Context context) {
		float retVal = ChicagoValue.INVALID_FLOAT;
		
		try {
			retVal = Float.parseFloat(value);
			context.getCounter(Counter.NUMERIC_PARSES_GOOD).increment(1l);
		}
		catch (NumberFormatException nfe) {
			context.getCounter(Counter.NUMERIC_PARSES_BAD).increment(1l);
		}
		
		return retVal;
	}
}
