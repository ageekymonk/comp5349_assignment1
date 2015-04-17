package task1;

import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 * Created by ramz on 11/04/15.
 */
public class SortLocalityByPhotosReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    public int topN = 50;
    public int topNcount = 0;
    Text locality = new Text();

    public void setup(Context context){
        topN = Integer.parseInt(context.getConfiguration().get("mapper.topNLocality.count", String.valueOf(topN)));
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text value: values)
        {
            topNcount = topNcount + 1;

            // Top N received. So returning
            if (topNcount > topN)
            {
                return;
            }
            locality.set(value);
            context.write(locality, key);
        }
    }
}
