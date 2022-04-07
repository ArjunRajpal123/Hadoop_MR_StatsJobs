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

public class binsMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text word = new Text();
    DoubleWritable number = new DoubleWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        Configuration conf = context.getConfiguration();

        String ind = conf.get("index");
        int index = Integer.parseInt(ind);

        String m1 = conf.get("min");
        int min = Integer.parseInt(m1);
        String m2 = conf.get("max");
        int max = Integer.parseInt(m2);
        String n = conf.get("num");
        int num = Integer.parseInt(n);

        int range = max - min;

        int size = range/num;
        
        int score = Integer.parseInt(line[index]);

        for(int i = 1; i <= (num + 1); i++){
            int upper = size * i;
            if(score < upper && score >= (upper - size)){
                String val = " " + (upper - size) + "-" + upper + ": ";
                word.set(val);
                context.write(word, number);
            }
        }
        
        
        
    }
}
