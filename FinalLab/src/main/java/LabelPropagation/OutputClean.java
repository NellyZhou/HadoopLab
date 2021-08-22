package LabelPropagation;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class OutputClean {
    public static class OutputMapper extends Mapper<LongWritable, Text,Text,Text> {

        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] list = line[1].split("&");

            context.write(new Text(line[0]),new Text(list[0]));
        }
    }

    public static class OutputReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text>value,Context context) throws IOException, InterruptedException {
            Text out = null;
            for (Text text : value) {
                out = text;
            }
            context.write(key, new Text(out.toString()));
        }
    }

    public static void run(String InputPath, String OutputPath) throws Exception {
        Configuration jobconf = new Configuration();
        // 全局作业参数的传递
        //jobconf.set("centerPath", centerPath);
        Job job = Job.getInstance(jobconf, "LPInitialize");
        job.setJarByClass(InputInitial.class);
        // 设置split块最大64MB
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize",
                64 * 1024 * 1024);

        // 输入类型与路径设置
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(InputPath));

        // mapper
        job.setMapperClass(OutputClean.OutputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // reducer
        job.setReducerClass(OutputClean.OutputReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输出的路径设置
        FileOutputFormat.setOutputPath(job, new Path(OutputPath));

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        run(args[0], args[1]);
    }
}
