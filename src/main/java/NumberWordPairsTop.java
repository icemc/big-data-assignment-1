import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.*;
import java.util.regex.Pattern;

public class NumberWordPairsTop extends Configured implements Tool {

    public static class HMap extends Mapper<Object, Text, Text, IntWritable> {
        private TreeMap<Integer, String> localTopK = new TreeMap<>(Collections.reverseOrder());

        private static final Pattern WORD_PATTERN = Pattern.compile("^[a-z_-]{6,24}$");
        private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?[0-9]+([.,][0-9]+)?$", Pattern.MULTILINE);

        @Override
        public void map(Object key, Text value, Context context) throws NumberFormatException {
            String[] tokens = value.toString().toLowerCase().split("\\s+"); // Split on whitespaces

            if (tokens.length == 2) {
                String[] words = tokens[0].split(":");
                if (words.length == 2 && NUMBER_PATTERN.matcher(words[0]).matches() && WORD_PATTERN.matcher(words[1]).matches()) {

                    try {
                        int count = Integer.parseInt(tokens[1]);
                        localTopK.put(count, tokens[0]);

                        // Keep only the top-100 elements in memory
                        if (localTopK.size() > 100) {
                            localTopK.pollLastEntry(); // Remove smallest count
                        }
                    } catch (NumberFormatException e) {
                        // Ignore malformed lines
                    }


                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : localTopK.entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, String> globalTopK = new TreeMap<>(Collections.reverseOrder());

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            for (IntWritable value : values) {
                globalTopK.put(value.get(), key.toString());

                if (globalTopK.size() > 100) {
                    globalTopK.pollLastEntry(); // Keep only the top 100
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : globalTopK.entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "NumberWordPairsTop");
        job.setJarByClass(NumberWordPairsTop.class);

        job.setMapperClass(HMap.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Split comma-separated input paths
        String[] inputPaths = args[0].split(",");
        for (String path : inputPaths) {
            FileInputFormat.addInputPath(job, new Path(path.trim()));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Use a single reducer to find global top-100
        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        int ret = ToolRunner.run(new Configuration(), new NumberWordPairsTop(), args);

        long endTime = System.currentTimeMillis();
        System.out.println("Job Execution Time: " + (endTime - startTime) + " ms");
        System.exit(ret);
    }
}