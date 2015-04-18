package task3;

import common.PlaceTypeUserTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ramz on 18/04/15.
 */
public class UniqueUsersPerLocCombiner extends Reducer<PlaceTypeUserTuple, IntWritable, PlaceTypeUserTuple, IntWritable> {
    public static IntWritable one = new IntWritable(1);

    @Override
    protected void reduce(PlaceTypeUserTuple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,one);
    }
}