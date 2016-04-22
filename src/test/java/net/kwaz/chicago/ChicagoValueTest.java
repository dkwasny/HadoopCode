package net.kwaz.chicago;

import net.kwaz.WritableTestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.EnumSet;

public class ChicagoValueTest {

	@Test
	public void writeAndReadValue() throws IOException {
		ChicagoValue expected = new ChicagoValue(
			11.1f,
			10f,
			123,
			93.393f,
			227.1f,
			WindDirection.CALM,
			294.14959f,
			1f,
			338f,
			EnumSet.of(Event.FOG),
			"Some Conditions",
			155
		);

		ChicagoValue actual = new ChicagoValue();
		WritableTestHelper.writeToOutput(expected, actual);
		Assert.assertEquals(expected, actual);
	}

}
