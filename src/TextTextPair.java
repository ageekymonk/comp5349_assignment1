import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * A composite key used to perform Join
 * @author Ying Zhou
 *
 */
public class TextTextPair implements WritableComparable<TextTextPair>{

    private Text key;
    private Text tag;

    public Text getKey() {
        return key;
    }

    public void setKey(Text key) {
        this.key = key;
    }

    public void setKey(String key) {
        this.key.set(key);
    }

    public Text getTag() {
        return tag;
    }

    public void setTag(Text tag) {
        this.tag = tag;
    }

    public void setTag(String tag) {
        this.tag.set(tag);
    }

    public TextTextPair(){
        this.key = new Text();
        this.tag = new Text();
    }

    public TextTextPair(String key, String tag){
        this.key = new Text(key);
        this.tag = new Text(tag);
    }

    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        key.readFields(in);
        tag.readFields(in);

    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        key.write(out);
        tag.write(out);
    }

    @Override
    public int compareTo(TextTextPair other) {
        // TODO Auto-generated method stub
        int cmp = key.compareTo(other.key);
        if (cmp != 0) {
            return cmp;
        }
        return tag.compareTo(other.tag);
    }

    @Override
    public int hashCode() {
        return key.hashCode() *163 + tag.hashCode();
    }

    @Override
    public String toString() {
        return key.toString() + "\t" + tag.toString();
    }

    public boolean equals(Object other) {
        if (other instanceof TextTextPair) {
            TextTextPair tip = (TextTextPair) other;
            return key.equals(tip.key) && tag.equals(tip.tag);
        }
        return false;
    }
}