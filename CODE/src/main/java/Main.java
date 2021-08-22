import Reader.ReadMain;
import NameWeight.WeightMain;
import PageRank.PRMain;
import LabelPropagation.LPDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main {
    public static void main(String[] args) throws Exception
    {
        /**
         * main函数传参说明
         * 0：input path
         * 1：output path
         */

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        String inputPath = otherArgs[1];
        String outputPath = otherArgs[2];

        System.out.println("--------------" + inputPath);
        System.out.println("--------------" + outputPath);
        ReadMain.main(new String[]{inputPath, outputPath + "Task12/"});

        WeightMain.main(new String[]{outputPath + "Task12/", outputPath + "Task3/"});

        PRMain.main(new String[]{outputPath + "Task3/", outputPath + "Task4"});

        LPDriver.main(new String[]{outputPath + "Task3/", outputPath + "Final/"});

        System.exit(0);
    }
}
