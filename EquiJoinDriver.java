// WordCountDriver.java
import java.io.IOException;
import java.util.Date;
import java.util.Formatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EquiJoinDriver {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        

        Job job = new Job(conf, "EquiJoinOperation");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(EquiJoinMapper.class);
        job.setReducerClass(EquiJoinReducer.class);

        System.out.println(job.waitForCompletion(true));
    }
}