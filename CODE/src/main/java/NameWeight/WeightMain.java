package NameWeight;

import Reader.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WeightMain {
    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "WeightJob");
            job.setJarByClass(WeightMain.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));

            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(WeightComputer.WeightComputerMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TextPair.class);

            job.setNumReduceTasks(5);

            job.setReducerClass(WeightComputer.WeightComputerReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            Path output_dir = new Path(args[1]);
            FileSystem hdfs = output_dir.getFileSystem(conf);
            if (hdfs.isDirectory(output_dir)) {
                hdfs.delete(output_dir, true);
            }

            FileOutputFormat.setOutputPath(job, output_dir);

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}