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
        private Text word_value = new Text();
        
        private ArrayList<String> all_words;
        private Map<String, Integer> counts; 

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            all_words = new ArrayList<String>();
            counts = new HashMap<>();
        }
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            
            for (String w : words) {
                if (all_words.contains(w)) {
                    
                } else {
                    context.write(new Text("Hello"), new Text(word));
                    // append the word to the list
                    all_words.add(w);

                    if (counts.containsKey(Integer.toString(w.length()))) {
                        counts.put(Integer.toString(w.length()), counts.get(Integer.toString(w.length())) + 1);
                    } else {
                        counts.put(Integer.toString(w.length()), 1);
                    }
                }
            }
            
        }



        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the graph
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

            for (Text calculated_value : values) {
                result.set(calculated_value);
                
                context.write(key, result);
            }

            // Emit the key vertex and its list of adjacent vertices
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
