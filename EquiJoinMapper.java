// WordCountMapper.java
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EquiJoinMapper extends
        Mapper<LongWritable, Text, IntWritable, Text> {
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	
 
    	
    	String tokens [] = value.toString().split(",");
        Integer keycol = Integer.parseInt(tokens[1]);
        String relation=tokens[0];
        String val = ""+relation+"\t";
        if(tokens.length != 0)
         {
            for (int cnt = 2; cnt < tokens.length; cnt++)
               {    
               val = val + tokens[cnt] + "\t";
            }
            
        }

        context.write(new IntWritable(keycol), new Text(val));

    }
   }

