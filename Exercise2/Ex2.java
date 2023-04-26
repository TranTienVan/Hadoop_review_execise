package Exercise2;

import java.io.IOException;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Ex2 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text myword = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {            
            // Split the sentence into words
            String[] words = value.toString().split(" ");

           // Create a Map to store the counts
            Map<String, Integer> wordCount = new HashMap<>();

            // Iterate over the words and update the counts
            for (String word : words) {
                if (wordCount.containsKey(word)) {
                    wordCount.put(word, wordCount.get(word) + 1);
                } else {
                    wordCount.put(word, 1);
                }
            }

            // Print the word counts
            for (String word : wordCount.keySet()) {
                myword.set(word);
                context.write(myword, new IntWritable(wordCount.get(word).intValue()));
            }


        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count += 1;
            }
            float average = (float)sum/count;
            String formattedString = String.format("%.3f", average);
            result.set(formattedString);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Ex2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPaths(job, args[0] + "," + args[1] + "," + args[2]);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
// java -cp
// "/home/20120623/hadoop/hadoop-3.3.4/share/hadoop/common/*:/home/20120623/hadoop/hadoop-3.3.4/share/hadoop/mapreduce/*:."
// WordCount
