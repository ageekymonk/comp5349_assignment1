package task3;

import common.PlaceTypeTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ramz on 18/04/15.
 */
public class CountUniqueUsersReducer extends Reducer<PlaceTypeTuple, IntWritable, PlaceTypeTuple, IntWritable> {
    private IntWritable totalUsers = new IntWritable();
    @Override
    protected void reduce(PlaceTypeTuple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value: values)
        {
            count = count + value.get();
        }
        totalUsers.set(count);
        context.write(key, totalUsers);
    }
}
