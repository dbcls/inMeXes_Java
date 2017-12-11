package jp.dbcls.hadoop;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

public class ChunkRecordReader implements RecordReader<Text, Text> {

	@SuppressWarnings("deprecation")
	private LineRecordReader lineReader;
	private LongWritable lineKey;
	private Text lineValue;
	private Set<String> stopWords = new HashSet<String>();

	private void parseStopWords() {
		try {
			BufferedReader fis = new BufferedReader(new FileReader("/repository/corpora/MEDLINE/resources/stop_words.list"));
			String pattern = null;
			while ((pattern = fis.readLine()) != null) {
				stopWords.add(pattern);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the stopwords file : " + StringUtils.stringifyException(ioe));
		}
	}
	
	public ChunkRecordReader(JobConf job, FileSplit split) throws IOException {
		parseStopWords();
		lineReader = new LineRecordReader(job, split);
		lineKey = lineReader.createKey();
		lineValue = lineReader.createValue();
	}

	/*
	 *_pubmed_id=18772103:2009
	 *_ArticleTitle_
	 *[NP Adherence , anti-adherence ]
	 *, and
	 *[EOS]
	 *_AbstractText_
	 *[PP For ]
	 *[NP many pathogenic bacteria ]
	 *,
	 *[NP food-grade anti-infective agents ]
	 *[NP that ]
	 *[VP are ]
	 *[ADJP inexpensive and safe ]
	 *.
	 *[EOS]
	 *_pubmed_id_end_
	 */
	@SuppressWarnings("deprecation")
	@Override
	public boolean next(Text key, Text value) throws IOException {
		Pattern fst = Pattern.compile("^\\[[A-Z]+ ");
		Pattern ept = Pattern.compile("^\\[[A-Z]+ \\W*");
		Pattern lpt = Pattern.compile(" *[;:,.]+ ]$");
		Pattern chkNgrm = Pattern.compile("^(?:\\s*|[\\W\\d]*|\\([^\\)]+|[Pp]\\s?[=<](?:[\\d\\s.]+)?|[Ii]+)$");
		Pattern chkFirst = Pattern.compile("^\\[\\w+ [)>,.]"); 
		StringBuffer sb = new StringBuffer();
		Boolean isLineExist = false;
		while(lineReader.next(lineKey, lineValue)) {
			isLineExist = true;
			String ins = lineValue.toString().trim();
			if(ins.length() == 0) continue;
			if(ins.startsWith("_pubmed_id=")){
				String [] dockey = ins.substring(11).split(":", 2);
				if(dockey.length != 2){
					throw new IOException("Invalid record received. ("+ins.substring(11)+')');
				}
				key.set(dockey[0]+'\t'+dockey[1].substring(0, 4));
			}
			else if(ins.startsWith("_pubmed_id_end_")){
				value.set(sb.toString().trim());
				break;
			}
			else if(ins.startsWith("[EOS]")
					|| ins.contains("ABSTRACT TRUNCATED AT")
					//	|| !ins.startsWith("[")
					|| ins.startsWith("]")
					|| ins.startsWith("_ArticleTitle_")
					|| ins.startsWith("_AbstractText_")){
				// do nothing
			}
			else {
				Matcher fm = fst.matcher(ins);
				if(!fm.find()) {
					ins = "[NP " + ins + " ]";
				}
				//if(!ins.startsWith("["))	ins = "[NP " + ins + " ]";
				if(!ins.endsWith(" ]")) ins += " ]";
				ins = lpt.matcher(ins).replaceFirst(" ]");
				if(ins.length() > 7){
					String s = ins.substring(0, ins.length()-2).trim();
					Matcher fts = chkFirst.matcher(s);
					if(!fts.find()){
						s = ept.matcher(s).replaceFirst("");
						if(!stopWords.contains(s)){
							Matcher ngs = chkNgrm.matcher(s);
							if(!(s.length()<=1 || ngs.find()))	sb.append(ins+"\t");
						}
					}
				}
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