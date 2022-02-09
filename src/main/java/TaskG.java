import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class TaskG {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] line = {};
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                line =itr.nextToken().split(", ");
                word.set(line[1]);
                context.write(word, new IntWritable(Integer.parseInt(line[4])));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<Integer> times = new ArrayList<Integer>();
            for (IntWritable val : values) {
                times.add(val.get());
            }
            if(times.size()>1){
                Collections.sort(times);
                if((times.get(times.size()-1)- times.get(0)) < 500000) { //500000 is our measure of 14 days (not based on traditional time units)
                    context.write(key, new IntWritable(1));
                }
            } else {
                context.write(key, new IntWritable(1));
            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskG");
        job.setJarByClass(TaskG.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {

    }
}