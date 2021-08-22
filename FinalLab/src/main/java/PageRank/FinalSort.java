package PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FinalSort {
    public static class FSMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] terms = value.toString().split("\t"); // {name, PageRank#list}
            if (terms[1].contains("#")) { // {name, PageRank#list}
                String[] temp = terms[1].split("#"); // {PageRank, list}
                context.write(new DoubleWritable(Double.valueOf(temp[0])), new Text(terms[0]));
            }
        }
    }
    public static class FSReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                context.write(key, t);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        String inputPath=args[0],outputPath=args[1];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRank Final Sort");
        job.setJarByClass(FinalSort.class);
        job.setMapperClass(FinalSort.FSMapper.class);
        job.setReducerClass(FinalSort.FSReducer.class);
        job.setSortComparatorClass(Comp.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(false);
    }
}
