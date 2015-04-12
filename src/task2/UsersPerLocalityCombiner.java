package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ramz on 12/04/15.
 */
public class UsersPerLocalityCombiner extends Reducer<CLNUComparable, IntWritable, CLNUComparable, IntWritable> {
    public static IntWritable one = new IntWritable(1);
    @Override
    protected void reduce(CLNUComparable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, one);
    }
}
