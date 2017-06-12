package acacia.mapreproduce.api.use.test;

import acacia.mapreproduce.api.use.MapClass;
import acacia.mapreproduce.api.use.ReduceClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * Created by miaomiao on 6/12/2017.
 */
public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        //System.setProperty("hadoop.home.dir", "E:\\Hadoop\\hadoop-2.6.0\\hadoop-2.6.0\\");
        int exitCode = ToolRunner.run(new WordCount(),args);
        System.exit(exitCode);
    }

    public  int run(String[] strings) throws Exception {
        if (strings.length !=2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n",getClass().getSimpleName());
            return -1;
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        conf.set("mapred.job.tracker","bej301748.cn.oracle.com:50070");
        conf.set("fs.default.name","bej301748.cn.oracle.com:50070");
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCountForAcacia");
//        JobConf jobConf = new JobConf(new Configuration(),WordCount.class);
//        jobConf.setJarByClass(WordCount.class);
//        jobConf.setJobName("WordCountForAcacia");

        FileInputFormat.addInputPath(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        int returnValue = job.waitForCompletion(true)?0:1;
        if (job.isSuccessful()) {
            System.out.println("Job was successful!");
        } else if (!job.isSuccessful()) {
            System.out.println("Job was not successful!");
        }
        return  returnValue;
    }
}
