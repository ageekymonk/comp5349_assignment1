import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ramz on 12/04/15.
 */
public class TagsPerLocalityCombiner extends Reducer<TextTextPair, IntWritable, TextTextPair, IntWritable> {
    IntWritable total = new IntWritable();

    @Override
    protected void reduce(TextTextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        total.set(sum);
        context.write(key, total);
    }
}
