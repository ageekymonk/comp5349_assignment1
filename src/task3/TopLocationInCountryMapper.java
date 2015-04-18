package task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ramz on 18/04/15.
 */
public class TopLocationInCountryMapper  extends Mapper<Object, Text, CountryLocalityCountNameTuple, Text> {

    public static int LOCALITY_TYPE = 7;
    public static int NEIGHBOURHOOD_TYPE = 22;

    private CountryLocalityCountNameTuple opkey = new CountryLocalityCountNameTuple();
    private Text opvalue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 3){ // a not complete record with all data
            return;                 // don't emit anything
        }

        String place = dataArray[0];
        int placeType = Integer.parseInt(dataArray[1]);
        int count = Integer.parseInt(dataArray[2]);

        // country
        opkey.setCountry(place.substring(1, place.indexOf("/", 1)));

        // Locality
        if (placeType == LOCALITY_TYPE)
        {
            opkey.setLocality("");
            opkey.setName(place.substring(place.lastIndexOf("/")+1));
        }
        else if (placeType == NEIGHBOURHOOD_TYPE)
        {
            opkey.setLocality(place.substring(0, place.lastIndexOf("/")));
            opkey.setName(place.substring(place.lastIndexOf("/")+1));
        }

        // Count
        opkey.setCount(count);
        opvalue.set("" + dataArray[1] + ":" + dataArray[2] + ":" + dataArray[0]);

        context.write(opkey, opvalue);
    }
}
