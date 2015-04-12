package task2;

import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import task1.ComparePairByInteger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 * Created by ramz on 12/04/15.
 */
public class UsersPerLocalityReducer extends Reducer<CLNUComparable, IntWritable, Text, Text> {
    public static IntWritable one = new IntWritable(1);

    private HashMap<Pair<String, String>, Integer> uniqueUsersPerLocality = new HashMap<Pair<String, String>, Integer>();
    private HashMap<Pair<String, String>, Integer> uniqueUsersPerNbr = new HashMap<Pair<String, String>, Integer>();

    @Override
    protected void reduce(CLNUComparable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if (key.getNeighbourhood().toString().equals(""))
        {
            Pair<String, String> temp = new Pair<String,String>(key.getCountry().toString(), key.getLocality().toString());
            Integer currentCount = uniqueUsersPerLocality.get(temp);
            if (currentCount == null)
                currentCount = 0;
            uniqueUsersPerLocality.put(temp, currentCount+1);
        }
        else
        {
            Pair<String, String> temp = new Pair<String,String>(key.getCountry().toString() + "/" +
                    key.getLocality().toString(), key.getNeighbourhood().toString());
            Integer currentCount = uniqueUsersPerNbr.get(temp);
            if (currentCount == null)
                currentCount = 0;
            uniqueUsersPerNbr.put(temp, currentCount+1);

        }

    }

    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        TreeMap<String, PriorityQueue<Pair<Integer, String>> > topLocalityPerCountry = new TreeMap<String, PriorityQueue<Pair<Integer, String>>>();
        for(Map.Entry<Pair<String, String>, Integer> entry: uniqueUsersPerLocality.entrySet() ) {
            String country = entry.getKey().getKey();
            PriorityQueue<Pair<Integer, String>> tempPq;
            tempPq = topLocalityPerCountry.get(country);
            if ( tempPq == null)
            {
                tempPq = new PriorityQueue<Pair<Integer, String>>(10, new ComparePairByInteger());
            }
            tempPq.add(new Pair<Integer, String>(entry.getValue(), entry.getKey().getValue()));
            if (tempPq.size() > 10)
            {
                tempPq.poll();
            }
            topLocalityPerCountry.put(country,  tempPq);
        }

        TreeMap<String, PriorityQueue<Pair<Integer, String>> > topNbrPerLocality = new TreeMap<String, PriorityQueue<Pair<Integer, String>>>();
        for(Map.Entry<Pair<String, String>, Integer> entry: uniqueUsersPerNbr.entrySet() ) {
            String locality_with_country = entry.getKey().getKey();
            PriorityQueue<Pair<Integer, String>> tempPq;
            tempPq = topLocalityPerCountry.get(locality_with_country);
            if ( tempPq == null)
            {
                tempPq = new PriorityQueue<Pair<Integer, String>>(1, new ComparePairByInteger());
            }
            tempPq.add(new Pair<Integer, String>(entry.getValue(), entry.getKey().getValue()));
            if (tempPq.size() > 1)
            {
                tempPq.poll();
            }
            topNbrPerLocality.put(locality_with_country,  tempPq);
        }

        for(Map.Entry<String, PriorityQueue<Pair<Integer, String>> > entry: topLocalityPerCountry.entrySet())
        {
            Text country =  new Text(entry.getKey());
            StringBuffer op = new StringBuffer();
            while (entry.getValue().peek() != null)
            {
                Pair<Integer,String> topLocation = entry.getValue().poll();
                op.append(topLocation.getValue()+":"+topLocation.getKey().toString());
                PriorityQueue<Pair<Integer, String>> temp = topNbrPerLocality.get(country + "/" + topLocation.getValue());
                if (temp != null)
                {
                    Pair<Integer, String> nbrcount = temp.poll();
                    op.append(","+nbrcount.getValue().toString()+":"+nbrcount.getKey());
                }
                op.append(" ");
            }

            Text output = new Text(op.toString().trim());
            context.write(country, output);
        }

    }

}
