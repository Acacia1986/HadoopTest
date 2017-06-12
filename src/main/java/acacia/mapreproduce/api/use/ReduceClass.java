package acacia.mapreproduce.api.use;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by miaomiao on 6/12/2017.
 */
public class ReduceClass extends Reducer {
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        Iterator valuesIt = values.iterator();
        while (valuesIt.hasNext()) {
            sum = sum + (Integer)valuesIt.next();
           // Object target = valuesIt.next();
           //context.write(key,target);
        }
        context.write(key,new IntWritable(sum));

    }
}
