package net.kwaz;

import org.apache.hadoop.io.Writable;

import java.io.*;

public class WritableTestHelper {

	public static <T extends Writable> T writeToOutput(T input, T output) throws IOException {
		byte[] bytes;
		try (
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos)
		) {
			input.write(dos);
			bytes = baos.toByteArray();
		}

		try (
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			DataInputStream dis = new DataInputStream(bais)
		) {
			output.readFields(dis);
		}

		return output;
	}

}
