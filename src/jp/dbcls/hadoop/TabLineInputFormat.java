package jp.dbcls.hadoop;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TabLineInputFormat extends FileInputFormat<Text, Text> {
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return true;
	}

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit input, JobConf job, Reporter reporter)
	throws IOException {
		reporter.setStatus(input.toString());
		return new TabLineReader(job, (FileSplit)input);
	}
}
