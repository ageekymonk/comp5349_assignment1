import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ramz on 11/04/15.
 */
public class PlaceToLocalityMapper extends Mapper<Object, Text, Text, Text> {

    private Text placeId= new Text();
    private Text locality = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 7){ // a not complete record with all data
            return; // don't emit anything
        }
        String place = dataArray[6];
        int place_type_id = Integer.parseInt(dataArray[5]);
        placeId.set(dataArray[0]);

        String[] placeArray = place.split("/");
        if (placeArray.length >= 4) {
            locality.set(placeArray[3]);
            context.write(placeId, locality);
        }
    }
}
