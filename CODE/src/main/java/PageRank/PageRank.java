package PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import javax.naming.Context;

public class PageRank {
    private static class PRMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");//name   PR#name,weight;name,weight...
            String[] temp = line[1].split("#");//PR#name,weight;name,weight...
            double PR = Double.valueOf(temp[0]);//PR
            context.write(new Text(line[0]), new Text(temp[1]));//name  name,weight;name,weight...
            String[] NameAndWeight = temp[1].split(";");
            for (String str : NameAndWeight) {
                if (str.length() > 0 ) {
                    double weight = Double.valueOf(str.split(",")[1]);
                    String person = str.split(",")[0];
                    context.write(new Text(person), new Text("*#" + (weight * PR)));
                }
            }
        }
    }
    private static class PRReducer extends Reducer<Text, Text, Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            String list = "";
            for(Text v:values){
                if(v.toString().contains("*")){
                    String newPR=v.toString().split("#")[1];
                    sum=sum+Double.valueOf(newPR);
                }else{
                    list=v.toString();
                }
            }
            context.write(key, new Text(String.valueOf(1 - 0.85 + 0.85 * sum) + "#" + list));//0.85 Damping
        }
    }
    public static void main(String[] args) throws Exception {
        String inPath=args[0],outPath=args[1];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PRIter,out:"+outPath);
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRank.PRMapper.class);
        job.setReducerClass(PageRank.PRReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(false);
    }
}
