package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ramz on 19/04/15.
 */
public class ReverseCountSorter extends WritableComparator {
    protected ReverseCountSorter() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable obj1 = (IntWritable) a;
        IntWritable obj2 = (IntWritable) b;

        return -1 * obj1.compareTo(obj2);

    }
}
