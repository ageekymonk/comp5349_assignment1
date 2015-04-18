package task3;

import common.PlaceTypeTuple;
import common.PlaceTypeUserTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ramz on 18/04/15.
 */
public class UniqueUsersPerLocReducer extends Reducer<PlaceTypeUserTuple, IntWritable, PlaceTypeTuple, IntWritable> {
    public static IntWritable one = new IntWritable(1);
    private PlaceTypeTuple opkey = new PlaceTypeTuple();

    @Override
    protected void reduce(PlaceTypeUserTuple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        opkey.setPlace(key.getPlace());
        opkey.setPlaceType(key.getPlaceType());
        context.write(opkey, one);
    }
}