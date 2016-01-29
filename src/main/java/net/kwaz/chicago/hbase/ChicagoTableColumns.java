package net.kwaz.chicago.hbase;

import com.google.common.base.Function;
import net.kwaz.HadoopUtils;
import net.kwaz.chicago.ChicagoValue;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;

public enum ChicagoTableColumns {
	TEMPERATURE_FAHRENHEIT(
		HadoopUtils.bytes("temp"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(Float.toString(input.getTemperatureFahrenheit()));
			}
		}
	),
	DEW_POINT_FAHERENHEIT(
		HadoopUtils.bytes("dewpoint"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(Float.toString(input.getDewPointFahrenheit()));
			}
		}
	),
	HUMIDITY(
		HadoopUtils.bytes("humid"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(Integer.toString(input.getHumidity()));
			}
		}
	),
	SEA_LEVEL(
		HadoopUtils.bytes("sealevel"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(Float.toString(input.getSeaLevelInchesOfMercury()));
			}
		}
	),
	VISIBILITY(
		HadoopUtils.bytes("vis"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(Float.toString(input.getVisibilityMiles()));
			}
		}
	),
	WIND_DIRECTION(
		HadoopUtils.bytes("winddir"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(input.getWindDirection().name());
			}
		}
	),
	WIND_SPEED(
		HadoopUtils.bytes("windspd"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(Float.toString(input.getWindSpeedMph()));
			}
		}
	),
	GUST_SPEED(
		HadoopUtils.bytes("gustspd"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(Float.toString(input.getGustSpeedMph()));
			}
		}
	),
	PRECIPITATION(
		HadoopUtils.bytes("precip"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(Float.toString(input.getPrecipitationInches()));
			}
		}
	),
	EVENTS(
		HadoopUtils.bytes("events"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.enumSetToHumanReadableBytes(input.getEvents());
			}
		}
	),
	CONDITIONS(
		HadoopUtils.bytes("cond"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(input.getConditions());
			}
		}
	),
	WIND_DIRECTION_DEGREES(
		HadoopUtils.bytes("winddirdeg"),
		new Function<ChicagoValue, byte[]>() {
			@Override
			public byte[] apply(ChicagoValue input) {
				return HadoopUtils.bytes(Integer.toString(input.getWindDirectionDegrees()));
			}
		}
	);
	
	public static final byte[] CF = HadoopUtils.bytes("d");
	
	private final byte[] bytes;
	private final Function<ChicagoValue, byte[]> serializer;
	ChicagoTableColumns(byte[] pBytes, Function<ChicagoValue, byte[]> pSerializer) {
		this.bytes = pBytes;
		this.serializer = pSerializer;
	}
	
	public byte[] getBytes() {
		return bytes;
	}
	
	public void addToPut(Put pPut, ChicagoValue pValue) {
		pPut.add(CF, this.bytes, serializer.apply(pValue));
	}
	
	public KeyValue createKeyValue(byte[] pKey, ChicagoValue pValue) {
		return new KeyValue(pKey, CF, this.bytes, serializer.apply(pValue));
	}
}
