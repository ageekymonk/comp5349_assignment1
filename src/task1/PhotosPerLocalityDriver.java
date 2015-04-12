package task1;

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
public class PhotosPerLocalityDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: PhotosPerLocality <placein> <photosin> <out>");
            System.exit(2);
        }

        Path tmpFilterOut = new Path("tmpFilterOut");

        Job job = new Job(conf, "Photos Per Locality Job");
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
        job.setJarByClass(PhotosPerLocalityDriver.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(PhotosPerLocalityMapper.class);
        job.setCombinerClass(PhotosPerLocalityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(PhotosPerLocalityReducer.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[1]));
        TextOutputFormat.setOutputPath(job, tmpFilterOut);
        job.waitForCompletion(true);

        Path tmpTopLocalityOut = new Path("tmpTopLocalityOut");
        Job sortJob = new Job(conf, "Top 50 Locality Job");
        sortJob.setJarByClass(PhotosPerLocalityDriver.class);
        sortJob.setNumReduceTasks(1);
        sortJob.setMapperClass(SortLocalityByPhotosMapper.class);
        sortJob.setMapOutputKeyClass(IntWritable.class);
        sortJob.setMapOutputValueClass(Text.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(IntWritable.class);
        sortJob.setReducerClass(SortLocalityByPhotosReducer.class);
        TextInputFormat.addInputPath(sortJob, tmpFilterOut);
        TextOutputFormat.setOutputPath(sortJob, tmpTopLocalityOut);
        sortJob.waitForCompletion(true);

        FileSystem.get(conf).delete(tmpFilterOut, true);

        Job tagJob = new Job(conf, "Top 10 Tags for Top 50 Locality");

        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), tagJob.getConfiguration());

        FileSystem fs = FileSystem.get(conf);
        Path pathPattern = new Path(tmpTopLocalityOut,"part-r-[0-9]*");
        FileStatus[] list = fs.globStatus(pathPattern);
        for (FileStatus status : list) {
            DistributedCache.addCacheFile(status.getPath().toUri(), tagJob.getConfiguration());
        }

        tagJob.setJarByClass(PhotosPerLocalityDriver.class);
        tagJob.setNumReduceTasks(4);
        tagJob.setMapperClass(TagsPerLocalityMapper.class);
        tagJob.setCombinerClass(TagsPerLocalityCombiner.class);
        tagJob.setReducerClass(TagsPerLocalityReducer.class);
        tagJob.setPartitionerClass(TagPartitioner.class);
        tagJob.setMapOutputKeyClass(TextTextPair.class);
        tagJob.setMapOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(tagJob, new Path(otherArgs[1]));
        TextOutputFormat.setOutputPath(tagJob, new Path(otherArgs[2]));
        tagJob.waitForCompletion(true);

        FileSystem.get(conf).delete(tmpTopLocalityOut, true);
    }
}

