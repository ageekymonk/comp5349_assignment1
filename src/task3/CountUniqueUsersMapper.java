package task3;

import common.PlaceTypeTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ramz on 18/04/15.
 */
public class CountUniqueUsersMapper extends Mapper<Object, Text, PlaceTypeTuple, IntWritable> {

    private IntWritable userCount = new IntWritable();
    private PlaceTypeTuple opkey = new PlaceTypeTuple();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 3){ // a not complete record with all data
            return;                 // don't emit anything
        }

        opkey.setPlace(dataArray[0]);
        opkey.setPlaceType(Integer.parseInt(dataArray[1]));

        userCount.set(Integer.parseInt(dataArray[2]));

        context.write(opkey, userCount);
    }
}
