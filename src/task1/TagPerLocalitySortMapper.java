package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Created by ramz on 11/04/15.
 */
public class TagPerLocalitySortMapper extends Mapper<TextTextPair, IntWritable, LocationCountPair, Text> {

    private LocationCountPair outputkey = new LocationCountPair();
    @Override
    protected void map(TextTextPair key, IntWritable value, Context context) throws IOException, InterruptedException {
        outputkey.setLocation(key.getKey());
        outputkey.setCount(value);
        context.write(outputkey, key.getTag());
    }
}
