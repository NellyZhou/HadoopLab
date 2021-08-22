package PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;


public class Initialize {
    public static class IniMapper extends Mapper<LongWritable,Text,Text,Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input format:name空格name,weight;name,weight; key value
            String []str = value.toString().split(" ");
            context.write(new Text(str[0]),new Text(str[1]));
        }
    }
    public static class IniReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text>value,Context context) throws IOException, InterruptedException {
            String result = "";
            String initRank="1";
            result=result+initRank+"#";
            for (Text val:value){
                result += val.toString();
            }
            context.write(key,new Text(result));
        }
    }
    public static void main(String[]args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Initialize for PageRank");
        job.setJarByClass(Initialize.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(IniMapper.class);
        job.setReducerClass(IniReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
