package net.kwaz;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.EnumSet;

public class HadoopUtilsTest {

	private enum TestEnum {
		VALUE1,
		VALUE2,
		VALUE3,
		VALUE4,
	}

	@Test
	public void writeAndReadEnumSet() throws IOException {
		EnumSet<TestEnum> expected = EnumSet.of(TestEnum.VALUE1, TestEnum.VALUE3);

		byte[] bytes;
		try (
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos)
		) {
			HadoopUtils.writeEnumSet(expected, dos);
			bytes = baos.toByteArray();
		}

		EnumSet<TestEnum> actual;
		try (
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			DataInputStream dis = new DataInputStream(bais)
		) {
			actual = HadoopUtils.readEnumSet(dis, TestEnum.class);
		}

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void enumSetToHumanReadableBytes() {
		byte[] expected = "VALUE2|VALUE4".getBytes();
		EnumSet<TestEnum> input = EnumSet.of(TestEnum.VALUE2, TestEnum.VALUE4);
		byte[] actual = HadoopUtils.enumSetToHumanReadableBytes(input);
		Assert.assertArrayEquals(expected, actual);
	}

	@Test
	public void humanReadableBytesToEnumSet() {
		EnumSet<TestEnum> expected = EnumSet.of(TestEnum.VALUE1, TestEnum.VALUE4);
		byte[] input = "VALUE1|VALUE4".getBytes();
		EnumSet<TestEnum> actual = HadoopUtils.humanReadableBytesToEnumSet(input, TestEnum.class);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void writeAndReadHumanReadableBytes() {
		EnumSet<TestEnum> expected = EnumSet.of(TestEnum.VALUE1, TestEnum.VALUE2);
		EnumSet<TestEnum> actual = HadoopUtils.humanReadableBytesToEnumSet(
			HadoopUtils.enumSetToHumanReadableBytes(expected),
			TestEnum.class
		);
		Assert.assertEquals(expected, actual);
	}
}