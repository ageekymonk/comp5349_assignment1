package task3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ramz on 18/04/15.
 */
public class ReverseCountryLocalityCountSorter extends WritableComparator {
    protected ReverseCountryLocalityCountSorter() {
        super(CountryLocalityCountNameTuple.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CountryLocalityCountNameTuple obj1 = (CountryLocalityCountNameTuple)a;
        CountryLocalityCountNameTuple obj2 = (CountryLocalityCountNameTuple)b;

        if (obj1.getCountry().equals(obj2.getCountry()))
        {
            if (obj1.getLocality().equals(obj2.getLocality()))
            {
                if (obj1.getCount().equals(obj2.getCount()))
                {
                    return obj1.getName().compareTo(obj2.getName());
                }
                else
                {
                    return -1 * obj1.getCount().compareTo(obj2.getCount());
                }
            }
            else
            {
                return obj1.getLocality().compareTo(obj2.getLocality());
            }
        }
        else
        {
            return obj1.getCountry().compareTo(obj2.getCountry());
        }
    }
}
