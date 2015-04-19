package task3;

import common.PlaceTypeUserTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ramz on 18/04/15.
 */
public class UniqueUsersPerLocCombiner extends Reducer<PlaceTypeUserTuple, Text, PlaceTypeUserTuple, Text> {
    public Text one = new Text();

    @Override
    protected void reduce(PlaceTypeUserTuple key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key,values.iterator().next());
    }
}