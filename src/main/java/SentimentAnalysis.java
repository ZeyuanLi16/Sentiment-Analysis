import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SentimentAnalysis {

    public static class SentimentSplit extends Mapper<Object, Text, Text, IntWritable> {

        public Map<String, String> emotionLibiary = new HashMap<String, String>();
        //store libiary for map() to use


        @Override
        public void setup(Context context) throws IOException{

            //context : 与mapreduce交互的其他工具的媒介

            Configuration configuration = context.getConfiguration();
                //a series of setup
                    //including setting read from line or sentence
            String dicName =configuration.get("dictionary","");
                //get the path of the file
            BufferedReader br = new BufferedReader(new FileReader(dicName));

            String line = br.readLine();
            while(line!=null){
                String[] word_feeling = line.split("\t");
                emotionLibiary.put(word_feeling[0], word_feeling[1]);
                br.readLine();
            }

            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //Read data
                /*

                data in hdfs - read from hdfs
                Mapper<Object, Text, Text, IntWritable>
                    --text是mapreduce变形之后的string
                mapper's input key-value: Object, Text
                    object（key）是读进来的的文章中offset是什么 原来文章中的第几行（不常用）
                    text（value）默认是一行一行读进来的 读进来的data
                mapper's output: Text, IntWritable
                 */


            //split into words
                //value is one of the line read from data

            String[] words = value.toString().split("\\s+");

            //look up in sentiment library
                /*
                create sentiment library only once in setup()
                because map() will be executed many times, does not have to be create every time
                 */
            for(String word : words){
                if(emotionLibiary.containsKey(word.trim().toLowerCase())){
                    /*
                        write out the result to the reducer
                        write to the disk using ohter tools --- context
                        用mapreduce之外的工具

                        key is the emotion
                        valiue is 1, once

                        Mapper<Object, Text, Text, IntWritable>
                            output is the "Text, IntWritable"
                     */
                    context.write(new Text(emotionLibiary.get(word.toLowerCase())), new IntWritable(1));

                }

            }

            //write result in key-value pair


        }
    }

    public static class SentimentCollection extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            /*
                key and value
                    value is a list of the same key
                    key = positive
                    value = <1, 1, 1, 1, ...>
                reducer has a function of shuffle -- transport and sort
                    transport same emotion from different machines to one
                    sort same emotion together
             */

            //combine data from mapper - done by reducer
            //sum count for each sentiment

            int sum = 0;
            for(IntWritable value:values ){
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));


        }

    }

    public static void main(String[] args) throws Exception {

        /*
            main function is a driver
                deliver many parameters to the function
            save as a prototype
        */

        Configuration config = new Configuration();

        config.set("dictionary", args[2]);
        // args[2] is the file path, come from the command line
        //args[0] args[1] are set as a input and output

        Job job = Job.getInstance(config);
        //mapreducer run as a jar
        //we need to define its driver class
        //tell job what its mapper and reducer class
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentSplit.class);
        job.setReducerClass(SentimentCollection.class);

        //set output key
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //inout and output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //when to start
            //more than one mapreduce jobs, need to the last job to finish
        job.waitForCompletion(true);

    }
}
