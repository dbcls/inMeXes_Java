package jp.dbcls.hadoop;

import java.io.*;
//import java.util.HashSet;
//import java.util.Set;
//import java.util.regex.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.util.StringUtils;

public class GetNgramInputReader implements RecordReader<Text, Text> {

	private LineRecordReader lineReader;
	private LongWritable lineKey;
	private Text lineValue;
	
	public GetNgramInputReader(JobConf job, FileSplit split) throws IOException {
		lineReader = new LineRecordReader(job, split);
		lineKey = lineReader.createKey();
		lineValue = lineReader.createValue();
	}

	@Override
	public boolean next(Text key, Text value) throws IOException {
		Boolean isLineExist = false;
		StringBuffer sb = new StringBuffer();

		while(lineReader.next(lineKey, lineValue)) {
			isLineExist = true;
			String ins = lineValue.toString().trim();
			if(ins.length() == 0) continue;
			if(ins.startsWith("_pubmed_id=")){
				key.set(ins.substring(11));
			}
			else if(ins.startsWith("_pubmed_id_end_")){
				value.set(sb.toString().trim());
				break;
			}
			else if(ins.contains("ABSTRACT TRUNCATED AT")
					|| ins.startsWith("_ArticleTitle_")
					|| ins.startsWith("_AbstractText_")){
				// do nothing
			}
			else {
				sb.append(ins+'\n');
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
