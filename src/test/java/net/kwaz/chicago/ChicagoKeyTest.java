package net.kwaz.chicago;

import net.kwaz.WritableTestHelper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ChicagoKeyTest {

	@Test
	public void writeAndReadKey() throws IOException {
		ChicagoKey expected = new ChicagoKey(123, new DateTime(DateTimeZone.UTC));
		ChicagoKey actual = WritableTestHelper.writeToOutput(expected, new ChicagoKey());
		Assert.assertEquals(expected, actual);
	}

}
