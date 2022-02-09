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
import java.util.Collections;
import java.util.StringTokenizer;

public class TaskD {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();
        private Text value = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException { //Friends.csv
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().split(", ")[1]); //page id
                value.set("friends    1");
                context.write(word, value);
            }
        }
    }

    public static class TokenizerMapper2
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();
        private Text value = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException { //mypage.csv
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String[] input = itr.nextToken().split(", ");
                word.set(input[0]); //page id
                value.set("mypage    " + input[1]); //name
                context.write(word, value);
            }
        }
    }

    public static class CombinerFunc
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            String name = "";
            for (Text val : values) {
                String input[] = val.toString().split("    ");
                if(input[0].equals("friends")){
                    sum++;
                }
                else if(input[0].equals("mypage")){ //mypage
                    name = input[1];
                }
            }
            context.write(key, new Text(name +"    "+String.valueOf(sum)));
        }
    }

    public static class IntSumReducerNew
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            String name = "";
            for (Text val : values) {
                String input[] = val.toString().split("    ");
                sum+= Integer.parseInt(input[1]);
                name=input[0];
            }
            context.write(new Text(String.valueOf(sum)), new Text(name));
        }
    }

//    public static class IntSumReducerOld
//            extends Reducer<Text,Text,Text,Text> {
//
//        public void reduce(Text key, Iterable<Text> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            int sum = 0;
//            String name = "";
//            for (Text val : values) {
//                String input[] = val.toString().split("    ");
//                if(input[0].equals("friends")){
//                    sum++;
//                }
//                else if(input[0].equals("mypage")){ //mypage
//                    name = input[1];
//                }
//            }
//            context.write(new Text(String.valueOf(sum)), new Text(name));
//        }
//    }

    public void debug(String[] args) throws Exception {
        //to convert to simple solution, set reducer to IntSumReducerOld.class and do not set a combiner
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);
        job.setCombinerClass(CombinerFunc.class);
        job.setReducerClass(IntSumReducerNew.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class); //friends
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class); //mypage
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
    }
}