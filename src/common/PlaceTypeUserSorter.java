package common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ramz on 19/04/15.
 */
public class PlaceTypeUserSorter extends WritableComparator {
    protected PlaceTypeUserSorter()
    {
        super(PlaceTypeUserTuple.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PlaceTypeUserTuple obj1 = (PlaceTypeUserTuple) a;
        PlaceTypeUserTuple obj2 = (PlaceTypeUserTuple) b;

        if (obj1.getPlace().equals(obj2.getPlace()))
        {
            if (obj1.getPlaceType().equals(obj2.getPlaceType()))
            {
                return obj1.getUser().compareTo(obj2.getUser());
            }
            else
            {
                return obj1.getPlaceType().compareTo(obj2.getPlaceType());
            }
        }
        else
        {
            return obj1.getPlace().compareTo(obj2.getPlace());
        }
    }
}
