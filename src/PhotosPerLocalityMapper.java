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
 * Created by ramz on 11/04/15.
 */
public class PhotosPerLocalityMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Hashtable<String, String> placeTable = new Hashtable<String, String>();
    private Text locality = new Text();
    private IntWritable totalPhotos = new IntWritable();

    // get the distributed file and parse it
    public void setup(Context context)
            throws java.io.IOException, InterruptedException{

        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (cacheFiles != null && cacheFiles.length > 0) {
            String line;
            String[] tokens;
            BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
            try {
                while ((line = placeReader.readLine()) != null) {
                    tokens = line.split("\t");
                    if (tokens.length < 7){ // a not complete record with all data
                        continue; // don't emit anything
                    }
                    String place = tokens[6];

                    String[] placeArray = place.split("/");
                    if (placeArray.length >= 4) {
                        placeTable.put(tokens[0], placeArray[3]);
                    }
                }
                System.out.println("size of the place table is: " + placeTable.size());
            }
            finally {
                System.out.println("Finally Closing it");
                placeReader.close();
            }
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 6){ // a not complete record with all data
            return; // don't emit anything
        }

        String placeid = dataArray[4];
        String locality_name = placeTable.get(placeid);
        if (locality_name != null)
        {
            locality.set(locality_name);
            totalPhotos.set(1);
            context.write(locality, totalPhotos);
        }
    }
}
