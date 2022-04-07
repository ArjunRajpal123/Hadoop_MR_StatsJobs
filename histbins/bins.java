import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class bins {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        conf.set("index", args[2]);
        conf.set("min", args[3]);
        conf.set("max", args[4]);
        conf.set("num", args[5]);

        Job job = Job.getInstance(conf, "variance");
        job.setJarByClass(bins.class);
        job.setMapperClass(binsMapper.class);
        job.setCombinerClass(binsReducer.class);
        job.setReducerClass(binsReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}