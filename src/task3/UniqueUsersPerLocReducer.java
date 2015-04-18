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
public class UniqueUsersPerLocReducer extends Reducer<PlaceTypeUserTuple, Text, PlaceTypeTuple, IntWritable> {
    public IntWritable opval = new IntWritable();
    private PlaceTypeTuple opkey = new PlaceTypeTuple();


    @Override
    protected void reduce(PlaceTypeUserTuple key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String previous_user = new String("");
        int count = 0;
        for (Text value: values)
        {
            if (value.toString().equals(previous_user))
            {
                continue;
            }
            else
            {
                count = count + 1;
            }
            previous_user = value.toString();

        }
        opkey.setPlace(key.getPlace());
        opkey.setPlaceType(key.getPlaceType());
        opval.set(count);
        context.write(opkey, opval);

    }
}