package NameWeight;

import Reader.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class WeightComputer {
    public static class WeightComputerMapper extends Mapper<LongWritable, Text, Text, TextPair> {
        /**
        * key : offset
        * value : name1,name2,times
        * **/
        @Override
        protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
            // 获取姓名对和对应出现次数
            String[] v = value.toString().split(",");
            if (v.length == 3)
                context.write(new Text(v[0]), new TextPair(v[1], v[2]));
        }
    }

    public static class WeightComputerReducer extends Reducer<Text, TextPair, Text, NullWritable> {
        /**
        * key : name
        * value : [name1,times],[name2,times]...
        * **/
        protected void reduce(Text key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            StringBuilder sb = new StringBuilder();
            // reduce中的value只能直接遍历一次，因此需要另外存储
            Vector v = new Vector();
            Vector names = new Vector();
            // 统计对于同一姓名，其它姓名一共出现的次数
            for (TextPair pair : values) {
                cnt += pair.getTimes();
                v.add(pair.getTimes());
                names.add(pair.getName());
            }
            sb.append(key.toString() + " ");

            // 遍历value，计算比例并组成字符串
            Enumeration vEnum = v.elements();
            int i = 0;
            while (vEnum.hasMoreElements()) {
                int curNum = (int) vEnum.nextElement();
                double curWeight = (double) curNum / (double) cnt;
                sb.append(names.get(i) + "," + curWeight + ";");
                i += 1;
            }
            context.write(new Text(sb.toString()), NullWritable.get());
        }
    }
}
