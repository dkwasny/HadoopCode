package net.kwaz.chicago.hbase;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import net.kwaz.HadoopUtils;
import net.kwaz.chicago.ChicagoKey;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.util.hash.MurmurHash;
import org.joda.time.format.ISODateTimeFormat;

public class ChicagoHbaseKey {
	
	private final boolean hashPrefix;
	private final ChicagoKey key;
	
	public ChicagoHbaseKey(ChicagoKey pKey, boolean pHashPrefix) {
		this.key = pKey;
		this.hashPrefix = pHashPrefix;
	}
	
	public Put createPut() {
		return new Put(toBytes());
	}
	
	public byte[] toBytes() {
		StringBuilder builder = new StringBuilder();
		builder.append(Integer.toString(key.getZipCode()));
		builder.append(HadoopUtils.SEP);
		builder.append(ISODateTimeFormat.dateTimeNoMillis().print(key.getDateTimeUtc()));
		String stringKey = builder.toString();
		byte[] bytes = HadoopUtils.bytes(stringKey);
		if (hashPrefix) {
			int hash = MurmurHash.getInstance().hash(bytes);
			bytes = Bytes.concat(Ints.toByteArray(hash), new byte[]{HadoopUtils.SEP}, bytes);
		}
		return bytes;
	}
}
