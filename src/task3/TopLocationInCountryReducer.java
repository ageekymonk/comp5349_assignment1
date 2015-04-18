package task3;

import javafx.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ramz on 18/04/15.
 */
public class TopLocationInCountryReducer extends Reducer<CountryLocalityCountNameTuple, Text, Text, Text> {

    public static int LOCALITY_TYPE = 7;
    public static int NEIGHBOURHOOD_TYPE = 22;

    private Text opval = new Text();
    @Override
    protected void reduce(CountryLocalityCountNameTuple key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Pair<String, String>> locality = new LinkedList<Pair<String, String>>();
        HashMap<String, String> nbr = new HashMap<String, String>();

        StringBuffer output = new StringBuffer();
        for(Text value: values)
        {
            String[] arr = value.toString().split(":");
            if (Integer.parseInt(arr[0]) == LOCALITY_TYPE)
            {
                if (locality.size() < 10)
                {
                    Pair<String, String> temp = new Pair<String, String>(arr[2], key.getName()+":"+arr[1]);
                    locality.add(temp);
                }
            }
            else if (Integer.parseInt(arr[0]) == NEIGHBOURHOOD_TYPE)
            {
                String locality_name = arr[2].substring(0, arr[2].lastIndexOf("/"));
                if (!nbr.containsKey(locality_name))
                {
                    nbr.put(locality_name, key.getName()+":"+arr[1]);
                }

            }

        }
        while(locality.size() > 0)
        {
            Pair<String, String> locality_info = locality.remove(0);
            output.append(locality_info.getValue());
            if (nbr.containsKey(locality_info.getKey()))
            {
                output.append("," + nbr.get(locality_info.getKey()));
            }
            output.append(" ");

        }

        opval.set(output.toString());
        context.write(key.getCountry(), opval);

    }
}
