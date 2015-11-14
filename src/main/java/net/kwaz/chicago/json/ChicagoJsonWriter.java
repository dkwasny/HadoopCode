package net.kwaz.chicago.json;

import java.io.IOException;

import net.kwaz.chicago.ChicagoKey;
import net.kwaz.chicago.ChicagoValue;
import net.kwaz.chicago.Event;

import org.codehaus.jackson.JsonGenerator;
import org.joda.time.format.ISODateTimeFormat;

public class ChicagoJsonWriter {

	public void toJson(ChicagoKey key, ChicagoValue value, JsonGenerator generator) throws IOException {
		generator.writeStartObject();
		
		String id = Integer.toString(key.getZipCode())
				+ Long.toString(key.getDateTimeUtc().getMillis());
		generator.writeStringField("id", id);
		
		generator.writeNumberField("zipCode_i", key.getZipCode());
		
		String dateString = ISODateTimeFormat.dateTimeNoMillis().withZoneUTC().print(key.getDateTimeUtc());
		generator.writeStringField("dateTimeUtc_dt", dateString);
		
		generator.writeNumberField("tempF_f", value.getTemperatureFahrenheit());
		generator.writeNumberField("dewPointF_f", value.getDewPointFahrenheit());
		generator.writeNumberField("humidity_i", value.getHumidity());
		generator.writeNumberField("seaLevel_f", value.getSeaLevelInchesOfMercury());
		generator.writeNumberField("vis_f", value.getVisibilityMiles());
		generator.writeStringField("windDir_s", value.getWindDirection().name());
		generator.writeNumberField("windSpeed_f", value.getWindSpeedMph());
		generator.writeNumberField("gustSpeed_f", value.getGustSpeedMph());
		generator.writeNumberField("precip_f", value.getPrecipitationInches());
		
		generator.writeArrayFieldStart("events_ss");
		for (Event event : value.getEvents()) {
			generator.writeString(event.name());
		}
		generator.writeEndArray();
		
		generator.writeStringField("cond_s", value.getConditions());
		generator.writeNumberField("windDir_i", value.getWindDirectionDegrees());
		
		generator.writeEndObject();
	}
}
