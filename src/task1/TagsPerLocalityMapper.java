package task1;

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
 * Created by ramz on 11/04/15.
 */
public class TagsPerLocalityMapper extends Mapper<Object, Text, TextTextPair, IntWritable> {

    private Hashtable<String, String> placeTable = new Hashtable<String, String>();
    private Hashtable<String, Integer> topPlaces = new Hashtable<String, Integer>();
    private IntWritable tagCount = new IntWritable();
    private TextTextPair tagKey = new TextTextPair();
    public static final Log LOG = LogFactory.getLog(TagsPerLocalityMapper.class);

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
                    int place_type = Integer.parseInt(tokens[5]);
                    if (place_type == 7) {
                        placeTable.put(tokens[0], place);
                    }
                    else if (place_type == 22)
                    {
                        placeTable.put(tokens[0], place.substring(0, place.lastIndexOf("/")));
                    }
                }
                LOG.error("size of the place table is: " + placeTable.size());
            }
            finally {
                System.out.println("Finally Closing it");
                placeReader.close();
            }
        }

        if (cacheFiles != null && cacheFiles.length > 1) {
            String line;
            String[] tokens;
            BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[1].toString()));
            try {
                while ((line = placeReader.readLine()) != null) {
                    tokens = line.split("\t");
                    if (tokens.length < 2){ // a not complete record with all data
                        continue; // don't emit anything
                    }
                    String place = tokens[0];
                    topPlaces.put(tokens[0], Integer.parseInt(tokens[1]));
                }
                LOG.error("size of the place table is: " + topPlaces.size());
            }
            finally {
                System.out.println("Finally Closing it");
                placeReader.close();
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 6){ // a not complete record with all data
            return; // don't emit anything
        }
        String placeid = dataArray[4];
        String locality_name = placeTable.get(placeid);

        if ((locality_name != null) && topPlaces.containsKey(locality_name))
        {
            String[] tags = dataArray[2].split(" ");
            for(String tag: tags)
            {
                tagKey.setKey(locality_name);
                tagKey.setTag(tag);
                tagCount.set(1);
                context.write(tagKey, tagCount);
            }

        }
    }
}
