import java.io.File;
import java.io.FileNotFoundException;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.xerces.util.SynchronizedSymbolTable;

public class TaskB {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();
        private Text value = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().split(", ")[2]);
                value.set("access    1");
                context.write(word, value);
            }
        }
    }

    public static class TokenizerMapper2
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();
        private Text value = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] input = itr.nextToken().split(", ");
                word.set(input[0]);
                value.set("mypage    " + input[1] + "    " + input[2]);
                context.write(word, value);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        public static ArrayList<String> topRecords = new ArrayList<String>();
        public static ArrayList<Integer> sums = new ArrayList<Integer>();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            String name = "";
            String nationality = "";
            for (Text val : values) {
                String input[] = val.toString().split("    ");
                if(input[0].equals("access")){
                    sum += Integer.parseInt(input[1]);
                }
                else if(input[0].equals("mypage")){ //mypage
                    name = input[1];
                    nationality = input[2];
                }
            }

            if(sum>0){
                String str = name + ", " + nationality  + ", sum = " + sum + "\n";
                topRecords.add(str);
                sums.add(sum);
                if(topRecords.size() > 8){
                    int i = sums.indexOf(Collections.min(sums));
                    sums.remove(sums.get(i));
                    topRecords.remove(topRecords.get(i));
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int counter = 1;
            for(String str : topRecords){
                String id, name, nationality;
                String[] values = str.split(", ");
                name = values[0];
                nationality = values[1];
                context.write(new Text(String.valueOf(counter)), new Text(name+", "+nationality));
                counter++;
            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskB");
        job.setJarByClass(TaskB.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class); //access
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class); //mypage
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
    }
}