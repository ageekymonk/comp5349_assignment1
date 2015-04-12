import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ramz on 11/04/15.
 */
public class SortLocalityByPhotosMapper extends Mapper<Object, Text, IntWritable, Text> {

    Text locality = new Text();
    IntWritable photoCount = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] dataArray = value.toString().split("\t");
        locality.set(dataArray[0]);
        photoCount.set(Integer.parseInt(dataArray[1]));

        context.write(photoCount, locality);
    }
}
