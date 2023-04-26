package Exercise4;

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


public class Ex4 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text word_value = new Text("1");
        List<String> stringList = new ArrayList<>();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            
            for (String w : words) {
                // check if the word is already in the list
                if (stringList.contains(w)) {
                    
                } else {
                    // append the word to the list
                    stringList.add(w);
                    word.set(Integer.toString(w.length()));
                    context.write(word, word_value);    
                }
                
            }
            
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();      
        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            float sum = 0, current_count;
            for (Text val : values) {
                try {
                    current_count = Float.parseFloat(val.toString().split(" ")[0]);
                    sum += current_count;
                    // code that might throw an exception
                } catch (Exception e) {
                    // code to handle the exception
                }
                
                    
                
            }
            
            String formattedString = String.format("%.0f", sum);
            result.set(formattedString);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count3");
        job.setJarByClass(Ex4.class);
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
