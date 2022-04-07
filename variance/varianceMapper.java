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



public class varianceMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text var = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        Configuration conf = context.getConfiguration();
        String path = conf.get("path");
        String index = conf.get("index");

        int indexInt = Integer.parseInt(index);

        
        double meanValue = Double.parseDouble(path);
        double temp = Double.parseDouble(line[indexInt]);
        double delta = temp - meanValue;

        delta = (delta * delta);


        DoubleWritable number = new DoubleWritable(delta);
        var.set("Variance: ");
        context.write(var, number);
    }
}
