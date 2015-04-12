package task2;

import javafx.util.Pair;
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
import java.util.Hashtable;

/**
 * Created by ramz on 12/04/15.
 */
public class UsersPerLocalityMapper extends Mapper<Object, Text, CLNUComparable, IntWritable> {

    private Hashtable<String, Pair<String, String>> countryLocalityTable = new Hashtable<String, Pair<String, String>>();
    private Hashtable<String, Pair<String, String>> LocalityNbrTable = new Hashtable<String, Pair<String, String>>();

    public static final Log LOG = LogFactory.getLog(UsersPerLocalityMapper.class);
    public static final IntWritable one = new IntWritable(1);

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
                        continue; // don't emit anything
                    }
                    String place = tokens[6];

                    String[] placeArray = place.split("/");
                    if (placeArray.length == 4) {
                        countryLocalityTable.put(tokens[0], new Pair<String, String>(placeArray[1], placeArray[3]));
                    }
                    else if (placeArray.length == 5) {
                        countryLocalityTable.put(tokens[0], new Pair<String, String>(placeArray[1], placeArray[3]));
                        LocalityNbrTable.put(tokens[0], new Pair<String, String>(placeArray[1]+"/"+placeArray[3], placeArray[4]));
                    }
                }
                LOG.error("size of the Country Locality table is: " + countryLocalityTable.size());
                LOG.error("size of the Locality Neighbourhood table is: " + LocalityNbrTable.size());
            } finally {
                System.out.println("Finally Closing it");
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
        String placeid = dataArray[4];
        Pair<String, String> cl = countryLocalityTable.get(placeid);
        Pair<String, String> ln = LocalityNbrTable.get(placeid);
        if (cl != null)
            context.write(new CLNUComparable(cl.getKey(), cl.getValue(), "", dataArray[1]), one);

        if ((cl != null) && (ln != null))
            context.write(new CLNUComparable(cl.getKey(), cl.getValue(), ln.getValue(), dataArray[1]), one);
    }
}
