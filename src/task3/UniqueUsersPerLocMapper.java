package task3;
import common.PlaceTypeUserTuple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by ramz on 18/04/15.
 */
public class UniqueUsersPerLocMapper extends Mapper<Object, Text, PlaceTypeUserTuple, Text> {

    public static int LOCALITY_TYPE = 7;
    public static int NEIGHBOURHOOD_TYPE = 22;

    private PlaceTypeUserTuple opkey = new PlaceTypeUserTuple();
    private Text opVal = new Text();

    private HashMap<String, String> locIdToUrl = new HashMap<String, String>();
    private HashMap<String, String> nbrIdToUrl = new HashMap<String, String>();

    private List<String> placeDetails = new LinkedList<String>();

    public static final Log LOG = LogFactory.getLog(UniqueUsersPerLocMapper.class);

    // get the distributed file and parse it
    public void setup(Context context)
            throws java.io.IOException, InterruptedException {
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (cacheFiles != null && cacheFiles.length > 0) {
            String line;
            String[] tokens;
            BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
            try {
                while ((line = placeReader.readLine()) != null) {
                    tokens = line.split("\t");
                    if (tokens.length < 7) { // a not complete record with all data
                        LOG.error("Invalid Line: " + line);
                        continue; // don't emit anything
                    }

                    int place_type = Integer.parseInt(tokens[5]);
                    if (place_type == 7) {
                        locIdToUrl.put(tokens[0], tokens[6]);
                    }
                    else if (place_type == 22)
                    {
                        nbrIdToUrl.put(tokens[0], tokens[6]);
                    }

                }

                LOG.error("Locality " + locIdToUrl.size());
                LOG.error("nbrIdToUrl: " + nbrIdToUrl.size());
            } finally {
                placeReader.close();
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 6){ // a not complete record with all data
            return;                 // don't emit anything
        }

        if (locIdToUrl.containsKey(dataArray[4]))
        {
            opkey.setPlace(locIdToUrl.get(dataArray[4]));
            opkey.setUser(dataArray[1]);
            opkey.setPlaceType(LOCALITY_TYPE);
            opVal.set(dataArray[1]);
            context.write(opkey, opVal);
        }
        else if (nbrIdToUrl.containsKey(dataArray[4]))
        {
            // Output for Neighbourhood
            String nbrurl = nbrIdToUrl.get(dataArray[4]);
            opkey.setPlace(nbrurl);
            opkey.setUser(dataArray[1]);
            opkey.setPlaceType(NEIGHBOURHOOD_TYPE);
            opVal.set(dataArray[1]);

            context.write(opkey, opVal);

            // Output for Locality
            opkey.setPlace(nbrurl.substring(0, nbrurl.lastIndexOf("/")));
            opkey.setUser(dataArray[1]);
            opkey.setPlaceType(LOCALITY_TYPE);

            context.write(opkey, opVal);
        }

    }
}
