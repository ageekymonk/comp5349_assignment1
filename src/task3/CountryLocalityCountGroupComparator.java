package task3;

        import org.apache.hadoop.io.WritableComparable;
        import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ramz on 18/04/15.
 */
public class CountryLocalityCountGroupComparator extends WritableComparator {

    protected CountryLocalityCountGroupComparator() {
        super(CountryLocalityCountNameTuple.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CountryLocalityCountNameTuple obj1 = (CountryLocalityCountNameTuple)a;
        CountryLocalityCountNameTuple obj2 = (CountryLocalityCountNameTuple)b;

        return obj1.getCountry().compareTo(obj2.getCountry());
    }
}
