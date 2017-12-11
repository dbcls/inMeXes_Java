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

import yayamamo.*;

public class GetNgram extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {

		static { System.loadLibrary("IndriPorter"); }
		static enum Counters { INPUT_WORDS }

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();
		private int nGramSize;
		private boolean considerJinfo = false;
		private boolean porterStem = false;
		private boolean perDocument = false;

		private long numRecords = 0;
		private String inputFile;
		private Pattern repPat  = Pattern.compile("(?:-+>|\\([\\+-]+\\)|[\\+-]+/[\\+-]+)");
		private Pattern spDelim = Pattern.compile("[\\s/-]+");
		private Pattern chkNgrm = Pattern.compile("(?:^\\s+|^[\\W\\d]*$)");

		public void configure(JobConf job) {

			patternsToSkip.add("\\(ABSTRACT TRUNCATED AT \\d+ WORDS\\)");
			patternsToSkip.add("<s n=\"\\d+\">");
			patternsToSkip.add("[.?]?</s>");

			considerJinfo = job.getBoolean("getngram.consider.jinfo", false);
			nGramSize     = job.getInt("getngram.ngram.size", 2) - 1;
			caseSensitive = job.getBoolean("getngram.case.sensitive", true);
			porterStem    = job.getBoolean("getngram.stem.porter", false);
			perDocument   = job.getBoolean("getngram.per.document", false);
			inputFile     = job.get("map.input.file");

			if (job.getBoolean("getngram.skip.patterns", false)) {
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

		public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String lines = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

			for (String pattern : patternsToSkip) {
				lines = lines.replaceAll(pattern, "");
			}

			String [] bh = {"No_Headings"};
			String headInfo [] = key.toString().split(":");
			if(headInfo.length == 3)
				bh = headInfo[2].split("\\|");
			//else if (lines.length()==0 || lines.startsWith("_") || lines.matches("^\\s*$")) return;

			StringBuffer buffer = new StringBuffer();
			Matcher matcher = repPat.matcher(lines);
			while(matcher.find()){
				matcher.appendReplacement(buffer, matcher.group(0).replace('-','\u263A').replace('/','\u263B'));
			}
			matcher.appendTail(buffer);

			Set<String> terms = null;
			if( perDocument )
				terms = new HashSet<String>();
			SWIGTYPE_p_Porter_Stemmer p = null;
			if (porterStem)
				p = IndriPorter.new_Porter_Stemmer();
			String[] lns = buffer.toString().split("\\n");
			for( String ln : lns ) {
				if (porterStem)
					ln = ln.replaceAll("\\W+", " ").trim();
				String[] sp = spDelim.split(ln);
				for (int i = 0;i < sp.length; i++){
					sp[i] = sp[i].replace('\u263A','-').replace('\u263B','/');
					if (porterStem)
						sp[i] = IndriPorter.Porter_Stemmer_porter_stem_y(p, sp[i], 0, sp[i].length()-1);
				}

				// N-gram

				for (int i = nGramSize;i < sp.length; i++) {
					String s = "";
					for (int id = nGramSize; id > 0; id--){
						s += sp[i-id] + ' ';
					}
					s += sp[i].replaceFirst("[;:,]$", "");
					// if(s.indexOf(' ')==0 || s.matches("^[\\W\\d]*$")) continue;

					if(s.startsWith("(") && s.endsWith(")")){
						String ss = s.substring(1, s.length()-1);
						if(!(ss.contains("(") || ss.contains(")"))) s = ss;
					}
					if(s.startsWith("(") && !s.contains(")")
							|| s.startsWith("\"") && !s.substring(1).contains("\"")
							|| s.startsWith("'") && !s.substring(1).contains("'"))
						s = s.substring(1, s.length());
					if(s.endsWith(")") && !s.contains("(")
							|| s.endsWith("\"") && !s.substring(0, s.length()-1).contains("\"")
							|| s.endsWith("'") && !s.substring(0, s.length()-1).contains("'"))
						s = s.substring(0, s.length()-1);
					if(s.startsWith("\"") && s.endsWith("\"")
							|| s.startsWith("'") && s.endsWith("'"))
					{
						s = s.substring(1, s.length()-1);
					}
					Matcher Nmatcher = chkNgrm.matcher(s);
					if(Nmatcher.find()) continue;

					if(!considerJinfo){
						if(perDocument)
							terms.add(s);
						else{
							word.set(s);
							output.collect(word, one);
							reporter.incrCounter(Counters.INPUT_WORDS, 1);
						}
						continue;
					}
					for (String st : bh) {
						String w = st+'\t'+s;
						if(perDocument)
							terms.add(w);
						else{
							word.set(w);
							output.collect(word, one);
							reporter.incrCounter(Counters.INPUT_WORDS, 1);
						}
					}
				}
			}
			if(perDocument)
				for (String term : terms) {
					word.set(term);
					output.collect(word, one);
					reporter.incrCounter(Counters.INPUT_WORDS, 1);
				}

			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		static enum MyCounters { NUM_RECORDS }
		private String reduceTaskId;
		private static int minimumFreq;
		public void configure(JobConf job) {
			reduceTaskId = job.get("mapred.task.id");
			minimumFreq = job.getInt("getngram.ngram.minfreq", 10);
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

		JobConf conf = new JobConf(getConf(), GetNgram.class);
		conf.setJobName("getngram");

    	conf.setOutputKeyClass(Text.class);
    	conf.setOutputValueClass(IntWritable.class);

    	conf.setMapperClass(Map.class);
    	conf.setCombinerClass(Combine.class);
    	conf.setNumReduceTasks(40);
    	conf.setReducerClass(Reduce.class);

    	conf.setCompressMapOutput(true);
    	conf.setMapOutputCompressorClass(DefaultCodec.class);

    	//conf.setInputFormat(SequenceFileInputFormat.class);
    	//conf.setOutputKeyClass(IntWritable.class);
    	//conf.setOutputFormat(SequenceFileOutputFormat.class);
    	//SequenceFileOutputFormat.setCompressOutput(conf, true);
    	//SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    	//SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);

    	conf.setInputFormat(GetNgramInputFormat.class);
    	conf.setOutputFormat(TextOutputFormat.class);

    	List<String> other_args = new ArrayList<String>();
    	for (int i=0; i < args.length; ++i) {
    		if ("-skip".equals(args[i])) {
    			DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
    			conf.setBoolean("getngram.skip.patterns", true);
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
		int res = ToolRunner.run(new Configuration(), new GetNgram(), args);
		System.exit(res);
	}
}
