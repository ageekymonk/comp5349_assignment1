package task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ramz on 18/04/15.
 */
public class CountryLocalityCountPartitioner extends Partitioner<CountryLocalityCountNameTuple, Text> {
    @Override
    public int getPartition(CountryLocalityCountNameTuple countryLocalityCountTuple, Text text, int i) {
        return countryLocalityCountTuple.getCountry().hashCode() * 163;
    }
}
