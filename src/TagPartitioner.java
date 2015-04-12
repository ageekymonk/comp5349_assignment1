import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ramz on 11/04/15.
 */
public class TagPartitioner extends Partitioner<TextTextPair, IntWritable> {

@Override
    public int getPartition(TextTextPair key, IntWritable value, int numPartition) {
        // TODO Auto-generated method stub
        return (key.getKey().hashCode() & Integer.MAX_VALUE) % numPartition;
    }

}
