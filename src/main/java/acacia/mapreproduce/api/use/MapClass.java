package acacia.mapreproduce.api.use;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by miaomiao on 6/12/2017.
 */
public class MapClass extends Mapper<LongWritable, Text,Text,IntWritable> {
    private final static IntWritable intwriteable = new IntWritable(1);
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line," ");
        while (stringTokenizer.hasMoreTokens()) {
                text.set( stringTokenizer.nextToken());
                context.write(text,intwriteable);
        }
    }
}
