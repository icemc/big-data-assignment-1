import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Pattern;

public class HadoopWordPairs extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text pair = new Text();
		private Queue<String> window = new LinkedList<>(); // Sliding window for m=2

		private static final Pattern WORD_PATTERN = Pattern.compile("^[a-z_\\-]{6,24}$");
		private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?[0-9]+([.,][0-9]+)?$", Pattern.MULTILINE);

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] splitLine = value.toString().toLowerCase().split("[^a-z0-9.,_\\-]+");

			for (String token : splitLine) {
				if (WORD_PATTERN.matcher(token).matches() || (NUMBER_PATTERN.matcher(token).matches() && token.length() >= 4 && token.length() <= 16)) {

					// Create pairs with previous words in the window (m=1 and m=2)
					for (String prevWord : window) {
						pair.set(prevWord + ":" + token);
						context.write(pair, one);
					}

					// Maintain a sliding window of size 2
					if (window.size() == 2) {
						window.poll(); // Remove oldest word
					}
					window.add(token);
				}
			}
			// Clear window at end of each line to avoid cross-line pairs
			window.clear();
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable value : values)
				sum += value.get();

			context.write(key, new IntWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// Set the filesystem to local
		Configuration conf = this.getConf();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");

		Job job = Job.getInstance(conf, "HadoopWordPairs");
		job.setJarByClass(HadoopWordPairs.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Split comma-separated input paths
		String[] inputPaths = args[0].split(",");
		for (String path : inputPaths) {
			FileInputFormat.addInputPath(job, new Path(path.trim()));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		int ret = ToolRunner.run(new Configuration(), new HadoopWordPairs(), args);

		long endTime = System.currentTimeMillis();
		System.out.println("Job Execution Time: " + (endTime - startTime) + " ms");
		System.exit(ret);
	}
}
