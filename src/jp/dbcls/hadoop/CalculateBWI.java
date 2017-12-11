package jp.dbcls.hadoop;
// BWI: Buzz Word Index
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;

public class CalculateBWI {

	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		static enum Counters { INPUT_WORDS }
		private long numRecords = 0;
		private String inputFile;

		@Override
		public void configure(JobConf job) {
			inputFile = job.get("map.input.file");
		}

		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String [] dockey = key.toString().split("_");
			output.collect(new Text(dockey[1]), new Text(dockey[0]+':'+value.toString()));
			reporter.incrCounter(Counters.INPUT_WORDS, 1);
			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
		static enum MyCounters { NUM_RECORDS }
		private long numRecords = 0;
		private String reduceTaskId;

		@Override
		public void configure(JobConf job) {
			reduceTaskId = job.get("mapred.task.id");
		}

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			TreeMap<Integer, Integer> syf = new TreeMap<Integer, Integer>();
			while (values.hasNext()) {
				String [] yearfreq = values.next().toString().split(":");
				syf.put(Integer.parseInt(yearfreq[0]), Integer.parseInt(yearfreq[1]));
			}
			int fyear = syf.firstKey();
			int lyear = syf.lastKey();
			for(int term; (term = lyear - fyear) >= 4; fyear++){
				int sum = 0;
				int plusNine = fyear + ((term < 9)?term:9);
				for(int i = fyear; i <= plusNine; i++){
					if(syf.get(i) != null)
						sum += syf.get(i);
				}
				output.collect(new Text(key.toString()+'_'+fyear+'-'+plusNine), new IntWritable(sum));
				reporter.incrCounter(MyCounters.NUM_RECORDS, 1);
				if ((++numRecords % 100) == 0) {
					reporter.setStatus(reduceTaskId + " finished processing " + numRecords + " records " + "from the input file");
				}
			}
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {

		JobClient client = new JobClient();
		JobConf conf = new JobConf(jp.dbcls.hadoop.CalculateBWI.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setCompressMapOutput(true);
		conf.setMapOutputCompressorClass(DefaultCodec.class);

		conf.setNumMapTasks(40);
		conf.setMapperClass(Map.class);
		conf.setNumReduceTasks(40);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TabLineInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i=0; i < args.length; ++i) {
			other_args.add(args[i]);
		}

		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}