package LabelPropagation;

import java.util.ArrayList;
import java.io.IOException;

import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.LineReader;

public class CheckFunction {
    public static boolean isSameCenters(String centerPath,String tmpPath) throws IOException {
        // 比较tmp文件是否与center相同
        ArrayList<Double> pre_max_label_weight = getWeight(centerPath);
        ArrayList<Double> new_max_label_weight = getWeight(tmpPath);
        // 判断两个坐标是否相等
        boolean isSame = Iterators.elementsEqual(pre_max_label_weight.iterator(),
                new_max_label_weight.iterator());

        return isSame;
    }

    public static ArrayList<Double> getWeight(String centerPath) throws IOException{
        Path path = new Path(centerPath);
        Configuration configuration = new Configuration();
        // 获取HDFS文件系统
        FileSystem fileSystem = path.getFileSystem(configuration);
        // 输出设置
        ArrayList<Double> ans = new ArrayList<>();
        // 如果信息存储在目录中
        if (fileSystem.getFileStatus(path).isDir() ){
            // 获取目录中所有文件的状态
            FileStatus[] fileStatuses = fileSystem.listStatus(path);
            for (FileStatus tmp_state : fileStatuses){
                ans.addAll(getWeight(tmp_state.getPath().toString()));
            }
            return ans;
        }

        // 读取centers的信息
        FSDataInputStream inputStream = fileSystem.open(path);
        LineReader lineReader = new LineReader(inputStream, configuration);
        Text line = new Text();
        while(lineReader.readLine(line) > 0) {
            String tmp_data = line.toString().split("\t")[1].split("&")[2];
            Double tmp_weight = Double.parseDouble(tmp_data);
            ans.add(tmp_weight);
        }
        lineReader.close();
        return ans;
    }
}
