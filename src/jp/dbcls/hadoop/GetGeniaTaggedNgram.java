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

public class GetGeniaTaggedNgram extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {

		//static { System.loadLibrary("IndriPorter"); }
		static enum Counters { INPUT_WORDS }

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private boolean caseSensitive = true;
		private int nGramSize;
		//private boolean porterStem = false;

		private long numRecords = 0;
		private String inputFile;
		private Pattern repPat  = Pattern.compile("(?:-+>|\\([\\+-]+\\)|[\\+-]+/[\\+-]+)");
		private Pattern spDelim = Pattern.compile("\\t");
		private Pattern chkNgrm = Pattern.compile("(?:^\\s+|^[\\W\\d]*$)");
		private Pattern inValidH = Pattern.compile("^[\\s!#$%&*+\\).\\d-]*$");
		private Pattern inValidT = Pattern.compile("^[\\s!#$%&*+\\(.\\d-]*$");

		public void configure(JobConf job) {
			nGramSize     = job.getInt("getngram.ngram.size", 2) - 1;
			caseSensitive = job.getBoolean("getngram.case.sensitive", true);
			//porterStem    = job.getBoolean("getngram.stem.porter", false);
			inputFile     = job.get("map.input.file");
		}

		private String ngram_filter(String s){
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
			return s.trim();
		}

		public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String tokens = (caseSensitive) ? key.toString() : key.toString().toLowerCase();

			StringBuffer buffer = new StringBuffer();
			Matcher matcher = repPat.matcher(tokens);
			while(matcher.find()){
				matcher.appendReplacement(buffer, matcher.group(0).replace('-','\u263A').replace('/','\u263B'));
			}
			matcher.appendTail(buffer);

			//SWIGTYPE_p_Porter_Stemmer p = null;
			//if (porterStem)
			//	p = IndriPorter.new_Porter_Stemmer();
			//if (porterStem)
			//	ln = ln.replaceAll("\\W+", " ").trim();
			String[] tokns = spDelim.split(buffer.toString().trim());
			String[] poses = spDelim.split(value.toString().trim());
			for (int i = 0;i < tokns.length; i++){
				tokns[i] = tokns[i].replace('\u263A','-').replace('\u263B','/');
				//if (porterStem)
				//	sp[i] = IndriPorter.Porter_Stemmer_porter_stem_y(p, sp[i], 0, sp[i].length()-1);
			}

			// N-gram

			for (int i = nGramSize;i < tokns.length; i++) {
				String s = "";
				for (int id = nGramSize; id > 0; id--){
					s += tokns[i-id] + ' ';
				}
				s += tokns[i];
				s = s.trim();
				String afs = ngram_filter(s);
				if(!afs.contentEquals(s)) continue;
				Matcher Nmatcher = chkNgrm.matcher(s);
				if(Nmatcher.find()) continue;

				if( tokns[i-nGramSize].contentEquals("''")
						|| tokns[i].contentEquals("''")
						|| inValidH.matcher(tokns[i-nGramSize]).find()
						|| inValidT.matcher(tokns[i]).find() )
				{
					continue;
				}

				String nf = "", nl = "", ni = "";
				String cp1 = "", cp2 = "";
				nf = tokns[i-nGramSize];
				cp1 = poses[i-nGramSize];
				for (int id = nGramSize - 1; id > 0; id--){
					ni += tokns[i-id] + ' ';
				}
				nl = tokns[i];
				cp2 = poses[i];
				word.set(nf + '\t' + ni.trim() + '\t' + nl + '\t' + cp1 + '\t' + cp2);
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

		JobConf conf = new JobConf(getConf(), GetGeniaTaggedNgram.class);
		conf.setJobName("getngram");

    	conf.setOutputKeyClass(Text.class);
    	conf.setOutputValueClass(IntWritable.class);

    	conf.setMapperClass(Map.class);
    	conf.setCombinerClass(Combine.class);
    	conf.setNumReduceTasks(40);
    	conf.setReducerClass(Reduce.class);

    	conf.setCompressMapOutput(true);
    	conf.setMapOutputCompressorClass(DefaultCodec.class);

    	conf.setInputFormat(GetGeniaTaggedInputFormat.class);
    	conf.setOutputFormat(TextOutputFormat.class);

    	List<String> other_args = new ArrayList<String>();
    	for (int i=0; i < args.length; ++i) {
    		other_args.add(args[i]);
    	}

    	FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
    	FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

    	JobClient.runJob(conf);

    	return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GetGeniaTaggedNgram(), args);
		System.exit(res);
	}

}
