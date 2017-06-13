package acacia.mapreproduce.api.use;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by miaomiao on 6/12/2017.
 */
public class ReduceClass extends Reducer {
    //@Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //Iterator valuesIt = values.iterator();
        for (IntWritable val : values) {
            //sum = sum + (Integer)valuesIt.next();
           sum += val.get();
           // Object target = valuesIt.next();
           //context.write(key,target);
        }
        context.write(key,new IntWritable(sum));

    }
}
