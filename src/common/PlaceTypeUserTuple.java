package common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ramz on 18/04/15.
 */
public class PlaceTypeUserTuple implements WritableComparable<PlaceTypeUserTuple> {
    private Text place;
    private Text user;
    private IntWritable placeType;

    public IntWritable getPlaceType() {
        return placeType;
    }

    public void setPlaceType(IntWritable placeType) {
        this.placeType = placeType;
    }

    public void setPlaceType(int placeType)
    {
        this.placeType.set(placeType);
    }

    public Text getPlace() {
        return place;
    }

    public void setPlace(Text place) {
        this.place = place;
    }

    public Text getUser() {
        return user;
    }

    public void setUser(Text user) {
        this.user = user;
    }

    public void setUser(String user) { this.user.set(user); }

    public void setPlace(String place) { this.place.set(place); }

    @Override
    public String toString() {
        return place.toString() + "\t" + placeType.toString() + "\t" + user.toString();
    }

    public PlaceTypeUserTuple() {
        this.place = new Text();
        this.user = new Text();
        this.placeType = new IntWritable();
    }
    public PlaceTypeUserTuple(Text place, IntWritable type, Text user ) {
        this.place = place;
        this.placeType = type;
        this.user = user;
    }

    public PlaceTypeUserTuple(String place, int type, String user) {
        this.place.set(place);
        this.user.set(user);
        this.placeType.set(type);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        place.write(dataOutput);
        placeType.write(dataOutput);
        user.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        place.readFields(dataInput);
        placeType.readFields(dataInput);
        user.readFields(dataInput);
    }

    @Override
    public int compareTo(PlaceTypeUserTuple o) {
        if (this.getPlace().equals(o.getPlace()))
        {
            if (this.getPlaceType().equals(o.getPlaceType()))
            {
                return this.getUser().compareTo(o.getUser());
            }
            else
            {
                return this.getPlaceType().compareTo(o.getPlaceType());
            }
        }
        else
        {
            return this.getPlace().compareTo(o.getPlace());
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PlaceTypeUserTuple) {
            PlaceTypeUserTuple tip = (PlaceTypeUserTuple) other;
            return this.getPlace().equals(tip.getPlace()) && this.getPlaceType().equals(tip.getPlaceType())
                    && this.getUser().equals(tip.getUser());
        }
        return false;
    }
}
