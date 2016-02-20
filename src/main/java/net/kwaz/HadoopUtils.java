package net.kwaz;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumSet;

public class HadoopUtils {
	
	public static final char SEP = '|';
	
	public static void deleteFiles(Iterable<Path> pPaths, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		for (Path path : pPaths) {
			fs.delete(path, false);
		}
		fs.close();
	}
	
	public static <T extends Enum<T>> void writeEnumSet(EnumSet<T> events, DataOutput out) throws IOException {
		out.writeInt(events.size());
		for (T event : events) {
			out.writeInt(event.ordinal());
		}
	}
	
	public static <T extends Enum<T>> EnumSet<T> readEnumSet(DataInput in, Class<T> klass) throws IOException {
		EnumSet<T> retVal = EnumSet.noneOf(klass);
		int size = in.readInt();
		for (int i = 0; i < size; ++i) {
			int ordinal = in.readInt();
			retVal.add(klass.getEnumConstants()[ordinal]);
		}
		return retVal;
	}
	
	public static <T extends Enum<T>> byte[] enumSetToHumanReadableBytes(EnumSet<T> events) {
		boolean first = true;
		StringBuilder builder = new StringBuilder();
		for (T event : events) {
			if (first) {
				first = false;
			}
			else {
				builder.append(SEP);
			}
			builder.append(event.name());
		}
		return bytes(builder.toString());
	}
	
	public static <T extends Enum<T>> EnumSet<T> humanReadableBytesToEnumSet(byte[] pBytes, Class<T> klass) {
		EnumSet<T> retVal = EnumSet.noneOf(klass);
		String stringValue = string(pBytes);
		Iterable<String> values = Splitter.on(SEP).split(stringValue);
		for (String value : values) {
			retVal.add(Enum.valueOf(klass, value));
		}
		return retVal;
	}
	
	public static void writeString(String value, DataOutput out) throws IOException {
		out.writeInt(value.length());
		out.write(bytes(value));
	}
	
	public static String readString(DataInput in) throws IOException {
		int length = in.readInt();
		byte[] bytes = new byte[length];
		in.readFully(bytes);
		return string(bytes);
	}
	
	public static byte[] bytes(String pString) {
		return pString.getBytes(Charsets.UTF_8);
	}
	
	public static String string(byte[] pBytes) {
		return new String(pBytes, Charsets.UTF_8);
	}

	private static final String DEFAULT_LIB_FOLDER = "lib";

	public static void addLibsToClasspath(Job job) throws IOException {
		addLibsToClasspath(new Path(DEFAULT_LIB_FOLDER), job);
	}

	public static void addLibsToClasspath(Path directory, Job job) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] files = fs.globStatus(directory.suffix("/*.jar"));
		for (FileStatus file : files) {
			job.addFileToClassPath(file.getPath());
		}
	}
}
