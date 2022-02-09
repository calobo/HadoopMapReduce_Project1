import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class TaskE {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();
        private Text value = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] line =itr.nextToken().split(", ");
                word.set(line[1]);
                value.set(line[2]);
                context.write(word, value);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            Set<String> accessedPages = new HashSet<String>();
            for (Text val : values) {
                sum++;
                accessedPages.add(val.toString());
            }
            context.write(key, new Text("accssed "+sum+" pages"+", and accessed "+accessedPages.size()+" distinct pages"));
        }
    }



    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskE");
        job.setJarByClass(TaskE.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {

    }
}