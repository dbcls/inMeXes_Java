package jp.dbcls.hadoop;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class TabLineReader implements RecordReader<Text, Text> {
	@SuppressWarnings("deprecation")
	private LineRecordReader lineReader;
	private LongWritable lineKey;
	private Text lineValue;

	public TabLineReader(JobConf job, FileSplit split) throws IOException {
		lineReader = new LineRecordReader(job, split);
		lineKey = lineReader.createKey();
		lineValue = lineReader.createValue();
	}
/*
 * 2003_[NP MAP kinase ]   197
 * 2003_[NP MAP kinase activation ]        43
 * 2003_[NP MAP kinase activity ]  27
 * 2003_[NP MAP kinase cascade ]   18
 * 2003_[NP MAP kinase cascades ]  10
 * 2003_[NP MAP kinase inhibitor ] 15
 * 2003_[NP MAP kinase inhibitors ]        10
 * 2003_[NP MAP kinase kinase ]    30
 * 2003_[NP MAP kinase pathway ]   59
 * 2003_[NP MAP kinase pathways ]  20
 * 2003_[NP MAP kinase phosphorylation ]   12
 * 2003_[NP MAP kinase signaling ] 14
 * 2003_[NP MAP kinases ]  135
 * 2003_[NP MAP stones ]   11
 * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object, java.lang.Object)
 */
	@Override
	public boolean next(Text key, Text value) throws IOException {
		if (!lineReader.next(lineKey, lineValue)) {
			return false;
		}
		String [] pieces = lineValue.toString().split("\t");
		key.set(pieces[0]);
		value.set(pieces[1]);
		return true;
	}

	@Override
	public Text createKey() {
		return new Text("");
	}

	@Override
	public Text createValue() {
		return new Text("");
	}

	@Override
	public long getPos() throws IOException {
		return lineReader.getPos();
	}

	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	@Override
	public float getProgress() throws IOException {
		return lineReader.getProgress();
	}

}