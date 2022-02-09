import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class TaskH {
    public static long numFriendEntries = 20000000; //number of records in friends.csv
    public static long numUniqueIds = 200000; //number of records in mypage.csv
    public static double average = numFriendEntries/numUniqueIds; //always will be 10

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private String[] currentLine;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) { //loops thru lines
                numFriendEntries++;
                currentLine = itr.nextToken().split(", ");
                word.set(currentLine[1]);
                context.write(word, new IntWritable(1));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int numFriends = 0;
            for (IntWritable val : values) {
                numFriends += val.get();
            }
            if(numFriends>average){
                context.write(key, new IntWritable(numFriends));
            }
        }

    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskH");
        job.setJarByClass(TaskH.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
    }
}
