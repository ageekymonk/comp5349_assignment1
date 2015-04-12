package task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by ramz on 11/04/15.
 */
public class Task2Driver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: PhotosPerLocality <placein> <photosin> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Photos Per Country Job");
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
        job.setJarByClass(Task2Driver.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(UsersPerLocalityMapper.class);
        job.setCombinerClass(UsersPerLocalityCombiner.class);
        job.setCombinerClass(UsersPerLocalityCombiner.class);
        job.setMapOutputKeyClass(CLNUComparable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(UsersPerLocalityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[1]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        job.waitForCompletion(true);

    }
}

