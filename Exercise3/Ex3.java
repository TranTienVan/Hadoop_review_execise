package Exercise3;

import java.io.IOException;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Ex3 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text word_value = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            Map<String, Integer> counts = new HashMap<>();
            
            for (String w : words) {
                if (counts.containsKey(w)) {
                    counts.put(w, counts.get(w) + 1);
                } else {
                    counts.put(w, 1);
                }
            }
            
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                word.set(entry.getKey());
                word_value.set(entry.getValue().toString());
                context.write(word, word_value);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();      
        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            float max_count = -2147483648;
            float min_count = 2147483647;
            float current_count;
            for (Text val : values) {
                try {
                    current_count = Float.parseFloat(val.toString());
                    if (current_count > max_count){
                        max_count = current_count;
                    }

                    if (current_count < min_count){
                        min_count = current_count;  
                    }
                    // code that might throw an exception
                } catch (Exception e) {
                    // code to handle the exception
                }
                
                    
                
            }
            
            String formattedString = String.format("%.0f", max_count) + " " + String.format("%.0f", min_count);
            result.set(formattedString);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count3");
        job.setJarByClass(Ex3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job, args[0] + "," + args[1] + "," + args[2]);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
// java -cp
// "/home/20120623/hadoop/hadoop-3.3.4/share/hadoop/common/*:/home/20120623/hadoop/hadoop-3.3.4/share/hadoop/mapreduce/*:."
// WordCount
