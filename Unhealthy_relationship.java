package section04;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class Unhealthy_relationship {
    public static class GraphMapper extends Mapper<Object, Text, Text, Text> {
        private GraphSolver graph;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            graph = new GraphSolver();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] vertices = line.split(" ");

            String src = vertices[0];
            String dest = vertices[1];

            graph.addVertex(src);
            graph.addVertex(dest);
            graph.addEdge(src, dest);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the graph
            for (String vertex : graph.getAllVertices()) {
                String flag = graph.calculateDegree(vertex);
                context.write(new Text(vertex), new Text(flag));

            }
        }
    }

    public static class GraphReducer extends Reducer<Text, Text, Text, Text> {
        private Text value = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Concatenate all the vertices that the key vertex has edges to
            for (Text calculated_value : values) {
                value.set(calculated_value);
                break;
            }

            // Emit the key vertex and its list of adjacent vertices
            context.write(key, value);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "extended word count");
        job.setJarByClass(Unhealthy_relationship.class);
        job.setMapperClass(GraphMapper.class);
        job.setCombinerClass(GraphReducer.class);
        job.setReducerClass(GraphReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
