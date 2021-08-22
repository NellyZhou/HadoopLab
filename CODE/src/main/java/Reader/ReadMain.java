package Reader;

import org.ansj.library.DicLibrary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.net.URI;


public class ReadMain {
    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = new Configuration();
//            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//            conf.addResource(new Path("/home/hadoop/conf/core-site.xml"));
//            conf.set("fs.defaultFS", "hdfs://master001:9000");
            conf.set("nameFile", args[0] + "/person_name_list.txt");

            Job job = new Job(conf, "ReaderJob");
            job.setJarByClass(ReadMain.class);
//            job.setJarByClass(DicLibrary.class);

            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(NovelRead.ReaderMapper.class);
            job.setMapOutputKeyClass(TextPair.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setCombinerClass(NovelRead.ReaderCombiner.class);

            job.setNumReduceTasks(2);

            job.setReducerClass(NovelRead.ReaderReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
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