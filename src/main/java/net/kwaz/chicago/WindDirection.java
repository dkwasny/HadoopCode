package net.kwaz.chicago;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public enum WindDirection {
	CALM("Calm"),
	NORTH("North"),
	NORTH_EAST("NE"),
	NORTH_NORTH_EAST("NNE"),
	NORTH_WEST("NW"),
	NORTH_NORTH_WEST("NNW"),
	EAST("East"),
	EAST_NORTH_EAST("ENE"),
	EAST_SOUTH_EAST("ESE"),
	SOUTH("South"),
	SOUTH_EAST("SE"),
	SOUTH_SOUTH_EAST("SSE"),
	SOUTH_WEST("SW"),
	SOUTH_SOUTH_WEST("SSW"),
	WEST("West"),
	WEST_NORTH_WEST("WNW"),
	WEST_SOUTH_WEST("WSW"),
	VARIABLE("Variable"),
	UNKNOWN(null);
	
	private final String sourceValue;
	WindDirection(String pSourceValue) {
		this.sourceValue = pSourceValue;
	}
	
	private static final Map<String, WindDirection> sourceMap;
	static {
		ImmutableMap.Builder<String, WindDirection> builder = ImmutableMap.builder();
		for (WindDirection w : WindDirection.values()) {
			if (!Strings.isNullOrEmpty(w.sourceValue)) {
				builder.put(w.sourceValue, w);
			}
		}
		sourceMap = builder.build();
	}
	
	public static WindDirection fromSourceValue(String pSourceValue) {
		WindDirection retVal = sourceMap.get(pSourceValue);
		if (retVal == null) {
			retVal = UNKNOWN;
		}
		return retVal;
	}
}