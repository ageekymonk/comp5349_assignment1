package common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import task3.CountryLocalityCountNameTuple;

/**
 * Created by ramz on 19/04/15.
 */
public class PlaceTypeUserGroupComparator extends WritableComparator {

    protected PlaceTypeUserGroupComparator() {
        super(PlaceTypeUserTuple.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PlaceTypeUserTuple obj1 = (PlaceTypeUserTuple)a;
        PlaceTypeUserTuple obj2 = (PlaceTypeUserTuple)b;

        return obj1.getPlace().compareTo(obj2.getPlace());

    }
}
