package net.kwaz.chicago;

import net.kwaz.HadoopUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumSet;

public class ChicagoValue implements Writable {

	// Use a small, but unreasonable given the data, constant for null and NaN values.
	public static final int INVALID_INT = -10000;
	public static final float INVALID_FLOAT = -10000f;

	private static final float FIVE_OVER_NINE = 5f/9f;

	private float temperatureFahrenheit;
	private float dewPointFahrenheit;
	private int humidity;
	private float seaLevelInchesOfMercury;
	private float visibilityMiles;
	private WindDirection windDirection;
	private float windSpeedMph;
	private float gustSpeedMph; // can be NaN
	private float precipitationInches; // can be NaN
	private EnumSet<Event> events;
	private String conditions;
	private int windDirectionDegrees;

	public ChicagoValue() { }
	
	public ChicagoValue(
			float pTemperatureFahrenheit,
			float pDewPointFahrenheit,
			int pHumidity,
			float pSeaLevelInchesOfMercury,
			float pVisibilityMiles,
			WindDirection pWindDirection,
			float pWindSpeedMph,
			float pGustSpeedMph,
			float pPrecipitationInches,
			EnumSet<Event> pEvents,
			String pConditions,
			int pWindDirectionDegrees)
	{
		this.temperatureFahrenheit = pTemperatureFahrenheit;
		this.dewPointFahrenheit = pDewPointFahrenheit;
		this.humidity = pHumidity;
		this.seaLevelInchesOfMercury = pSeaLevelInchesOfMercury;
		this.visibilityMiles = pVisibilityMiles;
		this.windDirection = pWindDirection;
		this.windSpeedMph = pWindSpeedMph;
		this.gustSpeedMph = pGustSpeedMph;
		this.precipitationInches = pPrecipitationInches;
		this.events = pEvents;
		this.conditions = pConditions;
		this.windDirectionDegrees = pWindDirectionDegrees;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		temperatureFahrenheit = arg0.readFloat();
		dewPointFahrenheit = arg0.readFloat();
		humidity = arg0.readInt();
		seaLevelInchesOfMercury = arg0.readFloat();
		visibilityMiles = arg0.readFloat();
		windDirection = WindDirection.values()[arg0.readInt()];
		windSpeedMph = arg0.readFloat();
		gustSpeedMph = arg0.readFloat();
		precipitationInches = arg0.readFloat();
		events = HadoopUtils.readEnumSet(arg0, Event.class);
		conditions = HadoopUtils.readString(arg0);
		windDirectionDegrees = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeFloat(temperatureFahrenheit);
		arg0.writeFloat(dewPointFahrenheit);
		arg0.writeInt(humidity);
		arg0.writeFloat(seaLevelInchesOfMercury);
		arg0.writeFloat(visibilityMiles);
		arg0.writeInt(windDirection.ordinal());
		arg0.writeFloat(windSpeedMph);
		arg0.writeFloat(gustSpeedMph);
		arg0.writeFloat(precipitationInches);
		HadoopUtils.writeEnumSet(events, arg0);
		HadoopUtils.writeString(conditions, arg0);
		arg0.writeInt(windDirectionDegrees);
	}

	public float getTemperatureFahrenheit() {
		return temperatureFahrenheit;
	}

	public float getTemperatureCelsius() {
		return (temperatureFahrenheit - 32) * FIVE_OVER_NINE;
	}

	public float getDewPointFahrenheit() {
		return dewPointFahrenheit;
	}

	public float getDewPointCelsius() {
		return (dewPointFahrenheit - 32) * FIVE_OVER_NINE;
	}

	public int getHumidity() {
		return humidity;
	}

	public float getSeaLevelInchesOfMercury() {
		return seaLevelInchesOfMercury;
	}

	public float getVisibilityMiles() {
		return visibilityMiles;
	}

	public WindDirection getWindDirection() {
		return windDirection;
	}

	public float getWindSpeedMph() {
		return windSpeedMph;
	}

	public float getGustSpeedMph() {
		return gustSpeedMph;
	}

	public float getPrecipitationInches() {
		return precipitationInches;
	}

	public EnumSet<Event> getEvents() {
		return events;
	}

	public String getConditions() {
		return conditions;
	}

	public int getWindDirectionDegrees() {
		return windDirectionDegrees;
	}

	@Override
	public String toString() {
		return "WeatherEntryValue [temperatureFahrenheit="
				+ temperatureFahrenheit + ", dewPointFahrenheit="
				+ dewPointFahrenheit + ", humidity=" + humidity
				+ ", seaLevelInchesOfMercury=" + seaLevelInchesOfMercury
				+ ", visibilityMiles=" + visibilityMiles + ", windDirection="
				+ windDirection + ", windSpeedMph=" + windSpeedMph
				+ ", gustSpeedMph=" + gustSpeedMph + ", precipitationInches="
				+ precipitationInches + ", events=" + events + ", conditions="
				+ conditions + ", windDirectionDegrees=" + windDirectionDegrees
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((conditions == null) ? 0 : conditions.hashCode());
		result = prime * result + Float.floatToIntBits(dewPointFahrenheit);
		result = prime * result + ((events == null) ? 0 : events.hashCode());
		result = prime * result + Float.floatToIntBits(gustSpeedMph);
		result = prime * result + humidity;
		result = prime * result + Float.floatToIntBits(precipitationInches);
		result = prime * result + Float.floatToIntBits(seaLevelInchesOfMercury);
		result = prime * result + Float.floatToIntBits(temperatureFahrenheit);
		result = prime * result + Float.floatToIntBits(visibilityMiles);
		result = prime * result
				+ ((windDirection == null) ? 0 : windDirection.hashCode());
		result = prime * result + windDirectionDegrees;
		result = prime * result + Float.floatToIntBits(windSpeedMph);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ChicagoValue other = (ChicagoValue) obj;
		if (conditions == null) {
			if (other.conditions != null)
				return false;
		} else if (!conditions.equals(other.conditions))
			return false;
		if (Float.floatToIntBits(dewPointFahrenheit) != Float
				.floatToIntBits(other.dewPointFahrenheit))
			return false;
		if (events == null) {
			if (other.events != null)
				return false;
		} else if (!events.equals(other.events))
			return false;
		if (Float.floatToIntBits(gustSpeedMph) != Float
				.floatToIntBits(other.gustSpeedMph))
			return false;
		if (humidity != other.humidity)
			return false;
		if (Float.floatToIntBits(precipitationInches) != Float
				.floatToIntBits(other.precipitationInches))
			return false;
		if (Float.floatToIntBits(seaLevelInchesOfMercury) != Float
				.floatToIntBits(other.seaLevelInchesOfMercury))
			return false;
		if (Float.floatToIntBits(temperatureFahrenheit) != Float
				.floatToIntBits(other.temperatureFahrenheit))
			return false;
		if (Float.floatToIntBits(visibilityMiles) != Float
				.floatToIntBits(other.visibilityMiles))
			return false;
		if (windDirection != other.windDirection)
			return false;
		if (windDirectionDegrees != other.windDirectionDegrees)
			return false;
		if (Float.floatToIntBits(windSpeedMph) != Float
				.floatToIntBits(other.windSpeedMph))
			return false;
		return true;
	}
}
