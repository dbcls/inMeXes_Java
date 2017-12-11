package jp.dbcls.hadoop;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class CountChunk extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {

		static enum Counters { INPUT_WORDS }

		private final static IntWritable one = new IntWritable(1);

		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();
		private boolean countByPMID = false;

		private long numRecords = 0;
		private String inputFile;
		private Pattern pmt = Pattern.compile("^(\\[[^\\(\\)]+)\\) \\]$");
		private Pattern apt = Pattern.compile("^\\[NP (?:[Aa]n?|[Tt]he) ");

		@Override
		public void configure(JobConf job) {
			caseSensitive = job.getBoolean("countchunk.case.sensitive", true);
			countByPMID = job.getBoolean("countchunk.count.by.pmid", false);
			inputFile = job.get("map.input.file");

			if (job.getBoolean("countchunk.skip.patterns", false)) {
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

		@Override
		public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			if(key.getLength()==0 || value.getLength()==0) return;

			String [] dockey = key.toString().split("\t");

			String chunks = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

			for (String pattern : patternsToSkip) {
				chunks = chunks.replaceAll(pattern, "");
			}

			for (String ce : chunks.split("\t")) {
				if(ce.length() == 0) continue;
				ce = apt.matcher(ce).replaceFirst("[NP ");
				Matcher mce = pmt.matcher(ce);
				if(mce.matches()) ce = mce.group(1).trim()+" ]";
				if(countByPMID){
					output.collect(new Text(dockey[0]+"_"+ce), one);
				}else{
					output.collect(new Text(dockey[1]+"_"+ce), one);
				}
				reporter.incrCounter(Counters.INPUT_WORDS, 1);
			}

			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		static enum MyCounters { NUM_RECORDS }
		private long numRecords = 0;
		private String reduceTaskId;
		private static int minimumFreq;

		@Override
		public void configure(JobConf job) {
			reduceTaskId = job.get("mapred.task.id");
			minimumFreq = job.getInt("countchunk.minfreq", 5);
		}

		@Override
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
		private long numRecords = 0;
		private String combineTaskId;

		@Override
		public void configure(JobConf job) {
			combineTaskId = job.get("mapred.task.id");
		}

		@Override
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

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CountChunk.class);
		conf.setJobName("countchunk");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setCompressMapOutput(true);
		conf.setMapOutputCompressorClass(DefaultCodec.class);

		conf.setNumMapTasks(40);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combine.class);
		conf.setNumReduceTasks(40);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(ChunkInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i=0; i < args.length; ++i) {
			if ("-skip".equals(args[i])) {
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
				conf.setBoolean("countchunk.skip.patterns", true);
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
		int res = ToolRunner.run(new Configuration(), new CountChunk(), args);
		System.exit(res);
	}
}