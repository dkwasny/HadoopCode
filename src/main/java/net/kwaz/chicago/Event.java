package net.kwaz.chicago;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public enum Event {
	FOG("Fog"),
	RAIN("Rain"),
	THUNDERSTORM("Thunderstorm"),
	SNOW("Snow"),
	HAIL("Hail"),
	UNKNOWN(null);
	
	private final String sourceValue;
	Event(String pSourceValue) {
		this.sourceValue = pSourceValue;
	}
	
	private static final Map<String, Event> sourceMap;
	static {
		ImmutableMap.Builder<String, Event> builder = ImmutableMap.builder();
		for (Event w : Event.values()) {
			if (!Strings.isNullOrEmpty(w.sourceValue)) {
				builder.put(w.sourceValue, w);
			}
		}
		sourceMap = builder.build();
	}
	
	public static Event fromSourceValue(String pSourceValue) {
		Event retVal = sourceMap.get(pSourceValue);
		if (retVal == null) {
			retVal = UNKNOWN;
		}
		return retVal;
	}
}