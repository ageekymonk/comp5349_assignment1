package task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ramz on 18/04/15.
 */
public class CountryLocalityCountNameTuple implements WritableComparable<CountryLocalityCountNameTuple> {
    private Text Country;
    private Text Locality;
    private IntWritable Count;
    private Text Name;

    public Text getName() {
        return Name;
    }

    public void setName(Text name) {
        Name = name;
    }

    public void setName(String name) {
        Name.set(name);
    }

    public Text getCountry() {
        return Country;
    }

    public void setCountry(Text country) {
        Country = country;
    }

    public void setCountry(String country) {
        Country.set(country);
    }

    public Text getLocality() {
        return Locality;
    }

    public void setLocality(Text locality) {
        Locality = locality;
    }

    public void setLocality(String locality) {
        Locality.set(locality);
    }

    public IntWritable getCount() {
        return Count;
    }

    public void setCount(IntWritable count) {
        Count = count;
    }

    public void setCount(int count) {
        Count.set(count);
    }

    public CountryLocalityCountNameTuple() {
        Country = new Text();
        Locality = new Text();
        Count = new IntWritable();
        Name = new Text();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Country.write(dataOutput);
        Locality.write(dataOutput);
        Count.write(dataOutput);
        Name.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Country.readFields(dataInput);
        Locality.readFields(dataInput);
        Count.readFields(dataInput);
        Name.readFields(dataInput);
    }

    @Override
    public int compareTo(CountryLocalityCountNameTuple o) {
        if (Country.equals(o.getCountry()))
        {
            if (Locality.equals(o.getLocality()))
            {
                if (Count.equals(o.getCount()))
                {
                    return Name.compareTo(o.getName());
                }
                else
                {
                    return -1 * Count.compareTo(o.getCount());
                }
            }
            else
            {
                return Locality.compareTo(o.getLocality());
            }
        }
        else
        {
            return Country.compareTo(o.getCountry());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CountryLocalityCountNameTuple) {
            CountryLocalityCountNameTuple other = (CountryLocalityCountNameTuple) obj;
            return Country.equals(other.getCountry()) && Locality.equals(other.getLocality())
                    && Count.equals(other.getCount()) && Name.equals(other.getName());
        }
        return false;
    }

    @Override
    public String toString() {
        return Country + "\t" + Locality + "\t" + Count + "\t" + Name;
    }
}
