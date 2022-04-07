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


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class stdev {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        
        conf.set("path", args[2]);

        conf.set("index", args[3]);

        Job job = Job.getInstance(conf, "variance");
        job.setJarByClass(stdev.class);

        job.setMapperClass(stdevMapper.class);
        //job.setCombinerClass(stdevReducer.class);
        job.setReducerClass(stdevReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}