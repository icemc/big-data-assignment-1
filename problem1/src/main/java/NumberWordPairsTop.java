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

public class NumberWordPairsTop extends Configured implements Tool {

    public static class HMap extends Mapper<Object, Text, Text, IntWritable> {
        private TreeMap<Integer, List<String>> localTopK = new TreeMap<>(Collections.reverseOrder());
        private int totalElements = 0;  // Track total elements

        @Override
        public void map(Object key, Text value, Context context) {
            String[] tokens = value.toString().toLowerCase().split(Utils.WHITESPACES);

            if (tokens.length == 2) {
                String[] pair = tokens[0].split(":");

                if (pair.length == 2 && Utils.NUMBER_PATTERN.matcher(pair[0]).matches() && Utils.WORD_PATTERN.matcher(pair[1]).matches()) {
                    try {
                        int count = Integer.parseInt(tokens[1]);
                        localTopK.putIfAbsent(count, new ArrayList<>());
                        localTopK.get(count).add(tokens[0]);
                        totalElements++;

                        // Remove extra elements to maintain exactly 100
                        while (totalElements > 100) {
                            Map.Entry<Integer, List<String>> lastEntry = localTopK.lastEntry();
                            if (lastEntry != null) {
                                List<String> words = lastEntry.getValue();
                                words.remove(words.size() - 1);
                                totalElements--;
                                if (words.isEmpty()) {
                                    localTopK.pollLastEntry(); // Remove empty group
                                }
                            }
                        }
                    } catch (NumberFormatException e) {
                        // Ignore malformed lines
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, List<String>> entry : localTopK.entrySet()) {
                for (String wordPair : entry.getValue()) {
                    context.write(new Text(wordPair), new IntWritable(entry.getKey()));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<Integer, List<String>> globalTopK = new TreeMap<>(Collections.reverseOrder());
        private int totalElements = 0;  // Track total elements

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            for (IntWritable value : values) {
                int count = value.get();
                globalTopK.putIfAbsent(count, new ArrayList<>());
                globalTopK.get(count).add(key.toString());
                totalElements++;

                // Remove extra elements to maintain exactly 100
                while (totalElements > 100) {
                    Map.Entry<Integer, List<String>> lastEntry = globalTopK.lastEntry();
                    if (lastEntry != null) {
                        List<String> words = lastEntry.getValue();
                        words.remove(words.size() - 1);
                        totalElements--;
                        if (words.isEmpty()) {
                            globalTopK.pollLastEntry(); // Remove empty group
                        }
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, List<String>> entry : globalTopK.entrySet()) {
                for (String wordPair : entry.getValue()) {
                    context.write(new Text(wordPair), new IntWritable(entry.getKey()));
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "NumberWordPairsTop");
        job.setJarByClass(NumberWordPairsTop.class);

        job.setMapperClass(HMap.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        String[] inputPaths = args[0].split(",");
        for (String path : inputPaths) {
            FileInputFormat.addInputPath(job, new Path(path.trim()));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

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
