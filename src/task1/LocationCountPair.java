package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ramz on 17/04/15.
 */
public class LocationCountPair implements WritableComparable<LocationCountPair> {

    private Text location;
    private IntWritable count;

    public Text getLocation() {
        return location;
    }

    public void setLocation(Text location) {
        this.location = location;
    }

    public void setLocation(String location) {
        this.location.set(location);
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    public void setCount(int count) {
        this.count.set(count);
    }

    public LocationCountPair() {
        this.location = new Text();
        this.count = new IntWritable();
    }

    public LocationCountPair(Text location, IntWritable count) {
        this.location = location;
        this.count = count;
    }

    public LocationCountPair(String location, int count) {
        this.location.set(location);
        this.count.set(count);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.location.write(dataOutput);
        this.count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.location.readFields(dataInput);
        this.count.readFields(dataInput);
    }

    @Override
    public int compareTo(LocationCountPair o) {
        int cmp = location.compareTo(o.location);
        if (cmp != 0) {
            return cmp;
        }
        return count.compareTo(o.count);
    }

    @Override
    public int hashCode() {
        return location.hashCode() * 163;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof LocationCountPair) {
            LocationCountPair tip = (LocationCountPair) other;
            return location.equals(tip.location) && count.equals(tip.count);
        }
        return false;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
