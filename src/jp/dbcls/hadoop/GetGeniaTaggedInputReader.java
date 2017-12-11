package jp.dbcls.hadoop;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

public class GetGeniaTaggedInputReader implements RecordReader<Text, Text> {
	
	private LineRecordReader lineReader;
	private LongWritable lineKey;
	private Text lineValue;
	
	public GetGeniaTaggedInputReader(JobConf job, FileSplit split) throws IOException {
		lineReader = new LineRecordReader(job, split);
		lineKey = lineReader.createKey();
		lineValue = lineReader.createValue();
	}

	@Override
	public boolean next(Text key, Text value) throws IOException {
		Boolean isLineExist = false;
//		StringBuffer sb = new StringBuffer();
//		ArrayList<String> tokArray = new ArrayList<String>();
//		ArrayList<String> posArray = new ArrayList<String>();

		while(lineReader.next(lineKey, lineValue)) {
			isLineExist = true;
			String ins = lineValue.toString().trim();
			if(ins.startsWith("TOK:")){
				key.set(ins.substring(4));
			}
			else if(ins.startsWith("CNK:")){
				break;
			}
			else if(ins.startsWith("POS:")){
				value.set(ins.substring(4));
			}
			else if(ins.startsWith("LEM:")){
				// do nothing
			}
			else {
				// do nothing
			}
			isLineExist = false;
		}
		return isLineExist;
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