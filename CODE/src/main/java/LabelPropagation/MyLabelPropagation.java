package LabelPropagation;

import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MyLabelPropagation {
    public static class LPMapper extends Mapper<LongWritable, Text, Text, Text> {

        // 计算每一个结点的新标签，向自身发送自身新信息，并向所有结点发送更新信息
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            ArrayList<String> update_name = new ArrayList<>();
            HashMap<String, Double> label_to_weight = new HashMap<>();
            String[] line = value.toString().split("\t");
            String neighbor_list = line[1].split("&")[1];
            String[] neighbors = neighbor_list.split(";");
            for (String str : neighbors){
                if (str.length() > 0){
                    String[] neighbors_info = str.split(",");
                    String neighbors_name = neighbors_info[0];
                    String neighbors_label = neighbors_info[1];
                    Double neighbors_weight = Double.parseDouble(neighbors_info[2]);
                    Double label_weight = label_to_weight.get(neighbors_label);
                    if (label_weight == null)
                        label_weight = neighbors_weight;
                    else
                        label_weight = label_weight + neighbors_weight;
                    if (!update_name.contains(neighbors_name))
                        update_name.add(neighbors_name);
                    label_to_weight.put(neighbors_label, label_weight);
                }
            }

            double max_weight = 0;
            String max_label = null;
            for (String str : label_to_weight.keySet()){
                if (label_to_weight.get(str) > max_weight){
                    max_label = str;
                    max_weight = label_to_weight.get(str);
                }
            }

            if (max_label != null){
                context.write(new Text(line[0]), new Text(max_label + "&" + neighbor_list + "&" + max_weight));
                for (String name : update_name){
                    context.write(new Text(name), new Text(line[0] + "/" + max_label));
                }
            }
        }
    }

    public static class LPReducer extends Reducer<Text, Text, Text, Text> {
        // 更新新的信息，并附加最大标签的权重
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
            HashMap<String, String> name_to_label = new HashMap<>();
            String new_label = null;
            String new_neighbor_list = null;
            String new_max_weight = null;
            for (Text text : values){
                String str = text.toString();
                if (str.contains("/")){
                    name_to_label.put(str.split("/")[0], str.split("/")[1]);
                } else {
                    new_label = str.split("&")[0];
                    new_neighbor_list = str.split("&")[1];
                    new_max_weight = str.split("&")[2];
                }
            }

            if (new_label != null && new_neighbor_list !=null && new_max_weight != null) {
                StringBuilder out = new StringBuilder();
                out.append(new_label);
                out.append("&");

                String[] neighbors = new_neighbor_list.split(";");
                for (String str : neighbors) {
                    String[] tmp_str = str.split(",");
                    out.append(tmp_str[0]);
                    out.append(",");
                    out.append(name_to_label.get(tmp_str[0]));
                    out.append(",");
                    out.append(tmp_str[2]);
                    out.append(";");
                }
                out.append("&");
                out.append(new_max_weight.toString());
                context.write(key, new Text(out.toString()));
            }
        }
    }



    public static void run(String dataPath, String outputPath) throws Exception {
        Configuration jobconf = new Configuration();
        // 全局作业参数的传递
        //jobconf.set("centerPath", centerPath);
        Job job = Job.getInstance(jobconf, "MyLabelPropagation");
        job.setJarByClass(MyLabelPropagation.class);
        // 设置split块最大64MB
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize",
                64 * 1024 * 1024);

        // 输入类型与路径设置
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(dataPath));

        // mapper
        job.setMapperClass(MyLabelPropagation.LPMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // reducer
        job.setReducerClass(MyLabelPropagation.LPReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输出的路径设置
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }

    public static int main(String[] args) throws Exception {
        int MAX_EPOCH_NUM = 10;
        int epoch = 0;

        String dataPath = args[0]; //输入目录
        String OutputPath = args[1];  //输出目录



        while (true) {
            epoch++;
            System.out.print("Epoch:======--"+epoch+"--======\n");
            run(dataPath, OutputPath + (epoch));
            if (CheckFunction.isSameCenters(dataPath, OutputPath + (epoch))) {
                System.out.print("Total training epoch: " + epoch + "\n");
                break;
            }
            if (epoch == MAX_EPOCH_NUM){
                System.out.print("Reached maximum epoch numbers.\n");
                break;
            }
            dataPath = OutputPath + (epoch);
        }
        return (epoch - 1);
    }

}
