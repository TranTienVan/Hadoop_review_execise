package submission_20120623;

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


public class Code_20120623 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text word_value = new Text();
        
        private Map<String, ArrayList<String>> counts; 

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            counts = new HashMap<>();
        }
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String word_key = value.toString();

            // if (word_key.contains("id") && !word_key.contains("")){
            //     ArrayList<String> current = new ArrayList<String>();
            //     counts.put(word_key, current);
            // }

            
            // for (String w : word_values) {
            //     if (counts.containsKey(w)) {
            //         ArrayList<String> current = counts.get(w);
            //         current.add(word_key);
            //         counts.put(w, current);
            //     } else {
            //         ArrayList<String> current = new ArrayList<String>();
            //         current.add(word_key);
            //         counts.put(w, current);
            //     }
                
            // }
            context.write(new Text(word_key), new Text(""));
            
        }



        // @Override
        // protected void cleanup(Context context) throws IOException, InterruptedException {
        //     // Output the graph
        //     for (Map.Entry<String, ArrayList<String>> entry : counts.entrySet()) {
        //         ArrayList<String> myList = entry.getValue();

        //         // Count the occurrences of each element in the ArrayList
        //         HashMap<String, Integer> countMap = new HashMap<String, Integer>();
        //         for (String element : myList) {
        //             if (countMap.containsKey(element)) {
        //                 countMap.put(element, countMap.get(element) + 1);
        //             } else {
        //                 countMap.put(element, 1);
        //             }
        //         }

        //         // Find the element with the highest count
        //         String mostFrequentElement = null;
        //         int highestCount = 0;
        //         for (Map.Entry<String, Integer> entry2 : countMap.entrySet()) {
        //             if (entry2.getValue() > highestCount) {
        //                 mostFrequentElement = entry2.getKey();
        //                 highestCount = entry2.getValue();
        //             }
        //         }


        //         word.set(entry.getKey());
        //         word_value.set(mostFrequentElement);
        //         context.write(word, word_value);
        //     }
        // }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();      
        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            

            
            for (Text calculated_value : values) {
                result.set(calculated_value);
                break;
            }

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count3");
        job.setJarByClass(Code_20120623.class);
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
