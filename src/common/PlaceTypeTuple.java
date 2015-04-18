package common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ramz on 18/04/15.
 */
public class PlaceTypeTuple implements WritableComparable<PlaceTypeTuple> {
    private Text place;
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

    public void setPlace(String place) { this.place.set(place); }

    @Override
    public String toString() {
        return place.toString() + "\t" + placeType.toString();
    }

    public PlaceTypeTuple() {
        this.place = new Text();
        this.placeType = new IntWritable();
    }
    public PlaceTypeTuple(Text place, IntWritable type ) {
        this.place = place;
        this.placeType = type;
    }

    public PlaceTypeTuple(String place, int type) {
        this.place.set(place);
        this.placeType.set(type);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        place.write(dataOutput);
        placeType.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        place.readFields(dataInput);
        placeType.readFields(dataInput);
    }

    @Override
    public int compareTo(PlaceTypeTuple o) {
        if (this.getPlace().equals(o.getPlace()))
        {
            return this.getPlaceType().compareTo(o.getPlaceType());

        }
        else
        {
            return this.getPlace().compareTo(o.getPlace());
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PlaceTypeTuple) {
            PlaceTypeTuple tip = (PlaceTypeTuple) other;
            return this.getPlace().equals(tip.getPlace()) && this.getPlaceType().equals(tip.getPlaceType());
        }
        return false;
    }
}
