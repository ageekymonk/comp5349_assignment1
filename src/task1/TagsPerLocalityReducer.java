package task1;

import javafx.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by ramz on 11/04/15.
 */
public class TagsPerLocalityReducer extends Reducer<TextTextPair, IntWritable, Text, Text> {

    private List<Pair<String, Integer>> topPlaces = new LinkedList<Pair<String, Integer>>();
    private Hashtable<String, PriorityQueue<Pair<Integer, String>>> placeTable = new Hashtable<String, PriorityQueue<Pair<Integer, String>>>();
    public static final Log LOG = LogFactory.getLog(TagsPerLocalityReducer.class);

    // get the distributed file and parse it
    public void setup(Reducer.Context context)
            throws java.io.IOException, InterruptedException{

        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());

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
                    topPlaces.add(new Pair<String, Integer>(tokens[0], Integer.parseInt(tokens[1])));
                }

            }
            finally {
                System.out.println("Finally Closing it");
                placeReader.close();
            }
        }
    }
    @Override
    protected void reduce(TextTextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        PriorityQueue<Pair<Integer, String>> tagMap = placeTable.get(key.getKey().toString());
        if (tagMap == null)
        {
            tagMap = new PriorityQueue<Pair<Integer, String>>(10, new ComparePairByInteger());
            placeTable.put(key.getKey().toString(), tagMap);
        }

        tagMap.add(new Pair<Integer,String>(sum, key.getTag().toString()));
        if (tagMap.size() > 10)
        {
            tagMap.poll();
        }
    }

    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        Text locality = new Text();
        Text tags = new Text();

        while (!topPlaces.isEmpty())
        {
            Pair<String, Integer> topPlace = topPlaces.remove(0);
            PriorityQueue<Pair<Integer, String>> tagQueue = placeTable.get(topPlace.getKey());
            StringBuffer taglist = new StringBuffer();
            while (tagQueue.peek() != null)
            {
                Pair<Integer,String> tag = tagQueue.poll();
                taglist.insert(0, tag.getValue() + ":" + tag.getKey().toString() + " ");
            }
            taglist.insert(0, topPlace.getKey() + "\t" + topPlace.getValue().toString() + "\t");
            tags.set(taglist.toString().trim());
            context.write(locality , tags  );
        }
    }
}
