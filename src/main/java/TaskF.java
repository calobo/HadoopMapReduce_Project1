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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class TaskF {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();
        private Text value = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException { //Friends.csv
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] line =itr.nextToken().split(", ");
                word.set(line[1]);
                value.set("friends    "+ line[2]);
                context.write(word, value);
            }
        }
    }

    public static class TokenizerMapper2
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();
        private Text value = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException { //accesslogs.csv
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] input = itr.nextToken().split(", ");
                word.set(input[1]); //page id
                value.set("access    " + input[2]); //accessed page id
                context.write(word, value);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            Set<String> friends = new HashSet<String>();
            Set<String> accessedPages = new HashSet<String>();
            for (Text val : values) {
                String input[] = val.toString().split("    ");
                if(input[0].equals("friends")){
                    if(!accessedPages.contains(input[1])){
                        friends.add(input[1].toString());
                    }
                }
                else if(input[0].equals("access")){
                    if(friends.contains(input[1])){
                        friends.remove(input[1]);
                    }
                    accessedPages.add(input[1]);
                }
            }
            if(!friends.isEmpty()){
                context.write(key, new Text("has unvisited friends"));
            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskF");
        job.setJarByClass(TaskF.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class); //friends
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class); //accesslogs
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {

    }
}