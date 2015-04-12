import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

    static PriorityQueue<Pair<Integer, String>> topNLocality = new PriorityQueue<Pair<Integer, String>>(50, new ComparePairByInteger());

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value: values)
        {
            topNLocality.add(new Pair<Integer, String>(key.get(), value.toString()));
            if (topNLocality.size() > 50)
            {
                topNLocality.poll();
            }
        }
    }

    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        IntWritable count = new IntWritable();
        Text locality = new Text();

        while(topNLocality.peek() != null)
        {
            Pair<Integer,String> value = topNLocality.poll();
            count.set(value.getKey());
            locality.set(value.getValue());
            context.write(locality, count);
        }
    }
}
