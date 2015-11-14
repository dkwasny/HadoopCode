package net.kwaz.chicago;

import com.google.common.base.Objects;
import com.google.common.primitives.Ints;
import org.apache.hadoop.io.WritableComparable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ChicagoKey implements WritableComparable<ChicagoKey> {
	
	private int zipCode;
	private DateTime dateTimeUtc;
	
	public ChicagoKey() { }
	
	public ChicagoKey(int pZipCode, DateTime pDateTimeUtc) {
		this.zipCode = pZipCode;
		this.dateTimeUtc = pDateTimeUtc;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		zipCode = arg0.readInt();
		dateTimeUtc = new DateTime(arg0.readLong(), DateTimeZone.UTC);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(zipCode);
		arg0.writeLong(dateTimeUtc.getMillis());
	}

	@Override
	public int compareTo(ChicagoKey arg0) {
		int compare = Ints.compare(zipCode, arg0.zipCode);
		if (compare == 0) {
			compare = dateTimeUtc.compareTo(arg0.dateTimeUtc);
		}
		return compare;
	}

	public int getZipCode() {
		return zipCode;
	}

	public DateTime getDateTimeUtc() {
		return dateTimeUtc;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dateTimeUtc == null) ? 0 : dateTimeUtc.hashCode());
		result = prime * result + zipCode;
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
		ChicagoKey other = (ChicagoKey) obj;
		return zipCode == other.zipCode
				&& Objects.equal(dateTimeUtc, other.dateTimeUtc);
	}

	@Override
	public String toString() {
		return "WeatherEntryKey [zipCode=" + zipCode + ", dateTimeUtc="
				+ dateTimeUtc + "]";
	}
}