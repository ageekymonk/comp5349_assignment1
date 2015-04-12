package task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ramz on 12/04/15.
 */
public class CLNUComparable implements WritableComparable<CLNUComparable> {

    private Text country;
    private Text locality;
    private Text neighbourhood;
    private Text user;

    public void setUser(Text user) {
        this.user = user;
    }

    public Text getCountry() {
        return country;
    }

    public void setCountry(Text country) {
        this.country = country;
    }

    public Text getLocality() {
        return locality;
    }

    public void setLocality(Text locality) {
        this.locality = locality;
    }

    public Text getNeighbourhood() {
        return neighbourhood;
    }

    public void setNeighbourhood(Text neighbourhood) {
        this.neighbourhood = neighbourhood;
    }

    public Text getUser() {
        return user;
    }

    public CLNUComparable() {
        this.country = new Text();
        this.locality = new Text();
        this.neighbourhood = new Text();
        this.user = new Text();
    }

    public CLNUComparable(String country, String locality, String neighbourhood, String user) {
        this.country = new Text(country);
        this.locality = new Text(locality);
        this.neighbourhood = new Text(neighbourhood);
        this.user = new Text(user);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof CLNUComparable) {
            CLNUComparable tip = (CLNUComparable) other;
            return this.getCountry().equals(tip.getCountry()) && this.getLocality().equals(tip.getLocality())
                    && this.getNeighbourhood().equals(tip.getNeighbourhood()) && this.getUser().equals(tip.getUser());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.getCountry().hashCode() * 163 + this.getLocality().hashCode() *163 +
                this.getNeighbourhood().hashCode() * 163 + this.getUser().hashCode() * 163;
    }

    @Override
    public int compareTo(CLNUComparable o) {
        if (this.getCountry().equals(o.getCountry()))
        {
            if (this.getLocality().equals(o.getLocality()))
            {
                if (this.getNeighbourhood().equals(o.getNeighbourhood()))
                {
                    return this.getUser().compareTo(o.getUser());
                }
                else
                {
                    return this.getNeighbourhood().compareTo(o.getNeighbourhood());
                }
            }
            else
            {
                return this.getLocality().compareTo(o.getLocality());
            }
        }
        else
        {
            return this.getCountry().compareTo(o.getCountry());
        }
    }

    @Override
    public String toString() {
        return this.getCountry()+"\t"+this.getLocality()+"\t"+this.getNeighbourhood()+"\t"+this.getUser();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.country.write(dataOutput);
        this.locality.write(dataOutput);
        this.neighbourhood.write(dataOutput);
        this.user.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.country.readFields(dataInput);
        this.locality.readFields(dataInput);
        this.neighbourhood.readFields(dataInput);
        this.user.readFields(dataInput);
    }
}
