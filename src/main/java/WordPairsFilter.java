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
import java.util.regex.Pattern;

public class WordPairsFilter extends Configured implements Tool {

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();
        private IntWritable count = new IntWritable();

        private static final Pattern WORD_PATTERN = Pattern.compile("^[a-z_-]{6,24}$");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().toLowerCase().split("\\s+"); // Split on whitespaces


            if (tokens.length == 2) {
                String[] words = tokens[0].split(":");
                if (words.length == 2 && WORD_PATTERN.matcher(words[0]).matches() && WORD_PATTERN.matcher(words[1]).matches()) {
                    try {
                        pair.set(tokens[0]); // Extract the word
                        count.set(Integer.parseInt(tokens[1])); // Extract the count
                        context.write(pair, count);
                    } catch (NumberFormatException e) {
                        // Ignore malformed lines
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                if (val.get() == 1000) { // Filter only words with count == 1000
                    context.write(key, val);
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // Set the filesystem to local
        Configuration conf = this.getConf();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "WordPairsFilter");
        job.setJarByClass(WordPairsFilter.class);

        job.setMapperClass(Map.class);
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

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int res = ToolRunner.run(new Configuration(), new WordPairsFilter(), args);

        long endTime = System.currentTimeMillis();
        System.out.println("Job Execution Time: " + (endTime - startTime) + " ms");
        System.exit(res);
    }
}