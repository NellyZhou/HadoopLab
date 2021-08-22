package Reader;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;


public class NovelRead {
    public static class ReaderMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            /**
             * 读取用户自定义字典
             */
            Configuration conf = context.getConfiguration();
            String nameFile = context.getConfiguration().get("nameFile");
//            System.out.print("--------" + nameFile);
            /*BufferedReader in = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(nameFile))));
             */
//            StringBuffer buffer = new StringBuffer();
            FileSystem fs = FileSystem.get(URI.create(nameFile), conf);
            FSDataInputStream fsr = fs.open(new Path(nameFile));
            BufferedReader in = new BufferedReader(new InputStreamReader(fsr));

            String name;
            while ((name = in.readLine()) != null) {
                DicLibrary.insert(DicLibrary.DEFAULT, name);
            }

            fsr.close();
            in.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /**
             * key : offset
             * value : 一行文件内容
             *
             * 读取小说内容，分词得到小说人物姓名，传出任意姓名对
             *
             * key : <name1,name2>
             * value : 1
             * **/
            String data = value.toString();
            Result res = DicAnalysis.parse(data);  // 得到分词结果
            List<Term> terms = res.getTerms();
            List<String> nameList = new ArrayList<String>();
            // 提取分词内容中的姓名
            for (int i = 0; i < terms.size(); i++) {
                String word = terms.get(i).toString();
                String wordNature = terms.get(i).getNatureStr();
                if (wordNature.equals(DicLibrary.DEFAULT_NATURE)) { // 存储符合用户自定义词典中的内容
                    String name = word.substring(0, word.length()-11);
                    if (name.equals("哈利波特") || name.equals("哈利·波特"))
                        name = "哈利";
                    else if (name.equals("赫敏·简·格兰杰") || name.equals("赫敏·格兰杰") || name.equals("赫敏格兰杰"))
                        name = "赫敏";
                    else if (name.equals("罗恩·比利尔斯·韦斯莱"))
                        name = "罗恩";
                    nameList.add(name);
                }
            }
            // 两两不相同姓名对之间进行统计
            int length = nameList.size();
            for (int i = 0; i < length; i++) {
                for (int j = 0; j < length; j++) {
                    if (!(nameList.get(i).equals(nameList.get(j))))
                        context.write(new TextPair(nameList.get(i), nameList.get(j)), new IntWritable(1));
                }
            }
        }
    }

    public static class ReaderCombiner extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            /**
             * 合并重复姓名对
             * **/
            int cnt = 0;
            for (IntWritable num : values) {
                cnt += num.get();
            }
            context.write(key, new IntWritable(cnt));
        }
    }

    public static class ReaderReducer extends Reducer<TextPair, IntWritable, Text, NullWritable> {
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            /**
             * key : <name1,name2>
             * values : times1,times2,...
             *
             * 统计同一个姓名对总共出现次数
             *
             * key : name1,name2,times
             * value : null
             * **/
            int cnt = 0;
            for (IntWritable num : values) {
                cnt += num.get();
            }
            context.write(new Text(key.toString() + "," + Integer.toString(cnt)), NullWritable.get());
        }
    }
}
