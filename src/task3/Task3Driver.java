package task3;

import common.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * Created by ramz on 18/04/15.
 */

public class Task3Driver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: UsersPerLocality <placein> <photosin> <out>");
            System.exit(2);
        }

        Path UniqueUsersPerLocalityPath = new Path("UniqueUsersPerLocalityPath");

        Job job = new Job(conf, "Unique Users Per Locality");
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
        job.setJarByClass(Task3Driver.class);
        job.setNumReduceTasks(4);
        job.setMapperClass(UniqueUsersPerLocMapper.class);


        job.setMapOutputKeyClass(PlaceTypeUserTuple.class);
        job.setMapOutputValueClass(Text.class);
        job.setSortComparatorClass(PlaceTypeUserSorter.class);
        job.setPartitionerClass(PlaceTypeUserPartitioner.class);
        job.setGroupingComparatorClass(PlaceTypeUserGroupComparator.class);

        job.setReducerClass(UniqueUsersPerLocReducer.class);
        job.setOutputKeyClass(PlaceTypeTuple.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, new Path(otherArgs[1]));
        TextOutputFormat.setOutputPath(job, UniqueUsersPerLocalityPath);
        job.waitForCompletion(true);

        Job topJob = new Job(conf, "Top 10 Location Per Country");
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), topJob.getConfiguration());
        topJob.setJarByClass(Task3Driver.class);
        topJob.setMapperClass(TopLocationInCountryMapper.class);
        topJob.setMapOutputKeyClass(CountryLocalityCountNameTuple.class);
        topJob.setMapOutputValueClass(Text.class);
        topJob.setPartitionerClass(CountryLocalityCountPartitioner.class);
        topJob.setSortComparatorClass(ReverseCountryLocalityCountSorter.class);
        topJob.setGroupingComparatorClass(CountryLocalityCountGroupComparator.class);
        topJob.setReducerClass(TopLocationInCountryReducer.class);
        topJob.setOutputKeyClass(Text.class);
        topJob.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(topJob, UniqueUsersPerLocalityPath);
        TextOutputFormat.setOutputPath(topJob, new Path(otherArgs[2]));

        topJob.waitForCompletion(true);
        FileSystem.get(conf).delete(UniqueUsersPerLocalityPath, true);
    }
}

