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

public class meanMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        Configuration conf = context.getConfiguration();
        String param = conf.get("index");
        int index = Integer.parseInt(param);
        
        DoubleWritable number = new DoubleWritable(Integer.parseInt(line[index]));
        word.set("Mean: ");
        context.write(word, number);
    }
}
