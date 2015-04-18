package common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ramz on 19/04/15.
 */
public class PlaceTypeUserPartitioner extends Partitioner<PlaceTypeUserTuple, Text> {

    @Override
    public int getPartition(PlaceTypeUserTuple placeTypeUserTuple, Text text, int i) {
        return (placeTypeUserTuple.getPlace().hashCode() & Integer.MAX_VALUE)%i;
    }
}
