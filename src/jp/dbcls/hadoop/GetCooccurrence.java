package jp.dbcls.hadoop;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.compress.*;

public class GetCooccurrence extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {

		static enum Counters { INPUT_WORDS }

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();
		private boolean isPMIDbase = false;
		private long numRecords = 0;
		private String inputFile;
		private Pattern repPat  = Pattern.compile("(?:-+>|\\([\\+-]+\\)|[\\+-]+/[\\+-]+)");
		private Pattern spDelim = Pattern.compile("[\\s/-]+");
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

		public void configure(JobConf job) {

			patternsToSkip.add("\\(ABSTRACT TRUNCATED AT \\d+ WORDS\\)");
			patternsToSkip.add("<s n=\"\\d+\">");
			patternsToSkip.add("[.?]?</s>");

			caseSensitive = job.getBoolean("getcooccurrence.case.sensitive", true);
			isPMIDbase    = job.getBoolean("getcooccurrence.PMIDbase", false);
			inputFile     = job.get("map.input.file");

			if (job.getBoolean("getcooccurrence.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = DistributedCache.getLocalCacheFiles(job);
				} catch (IOException ioe) {
					System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
				}
				for (Path patternsFile : patternsFiles) {
					parseSkipFile(patternsFile);
				}
			}

			parseStopWords();
		}

		private void parseSkipFile(Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
			}
		}

		private static void setWords(HashSet <String>hs, Text word, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			String[] hsp = new String[hs.size()];
			hs.toArray(hsp);

			for (int i = 0;i+1 < hsp.length; i++)
				for (int j = i+1;j < hsp.length; j++) {
					if(hsp[i].compareTo(hsp[j]) > 0){
						word.set(hsp[i]+'\t'+hsp[j]);
						output.collect(word, one);
					}else{
						word.set(hsp[j]+'\t'+hsp[i]);
						output.collect(word, one);
					}
					reporter.incrCounter(Counters.INPUT_WORDS, 1);
				}
		}

		public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String lines = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

			for (String pattern : patternsToSkip) {
				lines = lines.replaceAll(pattern, "");
			}

			// String headInfo [] = key.toString().split(":");
			// if(headInfo.length == 3){}

			StringBuffer buffer = new StringBuffer();
			Matcher matcher = repPat.matcher(lines);
			while(matcher.find()){
				matcher.appendReplacement(buffer, matcher.group(0).replace('-','\u263A').replace('/','\u263B'));
			}
			matcher.appendTail(buffer);

			String[] lns = buffer.toString().split("\\n");
			HashSet <String>hs = new HashSet<String>();
			for( String ln : lns ) {
				String[] sp = spDelim.split(ln);
				for (int i = 0;i < sp.length; i++){
					sp[i] = sp[i].replace('\u263A','-').replace('\u263B','/');
				}

				for (String s : sp) {
					String ss = s.replaceFirst("[;:,.]$", "");
					if(ss.matches("^[\\W\\d]*$")) continue;
					if(ss.startsWith("(") && ss.endsWith(")")){
						String sss = ss.substring(1, ss.length()-1);
						if(!(sss.contains("(") || sss.contains(")"))) ss = sss;
					}
					if(ss.startsWith("(") && !ss.contains(")")
							|| ss.startsWith("\"") && !ss.contains("\"")
							|| ss.startsWith("'") && !ss.contains("'"))
						ss = ss.substring(1, ss.length());
					if(ss.endsWith(")") && !ss.contains("(")
							|| ss.endsWith("\"") && !ss.contains("\"")
							|| ss.endsWith("'") && !ss.contains("'"))
						ss = ss.substring(0, ss.length()-1);
					if(ss.startsWith("\"") && ss.endsWith("\"")
							|| ss.startsWith("'") && ss.endsWith("'"))
					{   
						ss = ss.substring(1, ss.length()-1);
					}
					if((ss.startsWith("\"") && ss.indexOf('"', 1) < 0)
							|| (ss.startsWith("'")  && ss.indexOf("'", 1) < 0))
					{
						ss = ss.substring(1, ss.length());
					}
					if(ss.indexOf('"') == ss.length()-1
							|| ss.indexOf("'") == ss.length()-1)
					{
						ss = ss.substring(0, ss.length()-1);
					}
					if((ss.startsWith(".") && ss.indexOf('.', 1) < 0)
							|| (ss.startsWith("*") && ss.indexOf('*', 1) < 0))
					{
						continue;
					}
					if(ss.length() > 1 && !stopWords.contains(ss)) hs.add(ss);
				}

				if (!isPMIDbase) {
					setWords(hs, word, output, reporter);
					hs.clear();
				}
			}

			if (isPMIDbase)
				setWords(hs, word, output, reporter);

			if ((++numRecords % 100) == 0)
				reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		static enum MyCounters { NUM_RECORDS }
		private String reduceTaskId;
		private static int minimumFreq;
		public void configure(JobConf job) {
			reduceTaskId = job.get("mapred.task.id");
			minimumFreq = job.getInt("getcooccurrence.minfreq", 10);
		}

		private long numRecords = 0;
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			if(sum >= minimumFreq){
				output.collect(key, new IntWritable(sum));
				reporter.incrCounter(MyCounters.NUM_RECORDS, 1);
				if ((++numRecords % 100) == 0) {
					reporter.setStatus(reduceTaskId + " finished processing " + numRecords + " records " + "from the input file");
				}
			}
		}
	}

	public static class Combine extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		private String combineTaskId;
		public void configure(JobConf job) {
			combineTaskId = job.get("mapred.task.id");
		}

		private long numRecords = 0;
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
			if ((++numRecords % 100) == 0) {
				reporter.setStatus(combineTaskId + " finished processing " + numRecords + " records " + "from the input file");
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(getConf(), GetCooccurrence.class);
		conf.setJobName("getcooccurrence");

    	conf.setOutputKeyClass(Text.class);
    	conf.setOutputValueClass(IntWritable.class);

    	conf.setMapperClass(Map.class);
    	conf.setCombinerClass(Combine.class);
    	conf.setNumReduceTasks(40);
    	conf.setReducerClass(Reduce.class);

    	conf.setCompressMapOutput(true);
    	conf.setMapOutputCompressorClass(DefaultCodec.class);

    	conf.setInputFormat(GetNgramInputFormat.class);
    	conf.setOutputFormat(TextOutputFormat.class);

    	List<String> other_args = new ArrayList<String>();
    	for (int i=0; i < args.length; ++i) {
    		if ("-skip".equals(args[i])) {
    			DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
    			conf.setBoolean("getcooccurrence.skip.patterns", true);
    		} else {
    			other_args.add(args[i]);
    		}
    	}

    	FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
    	FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

    	JobClient.runJob(conf);

    	return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GetCooccurrence(), args);
		System.exit(res);
	}

}
