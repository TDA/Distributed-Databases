// WordCountReducer.java
import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordCountReducer extends
        Reducer<IntWritable,Text, IntWritable,Text> {
    protected void reduce(IntWritable key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
    	String str = "";
        for (Text val : values) 
                     {
        	String text_val[]=val.toString().split("\t");
        	for(int i=1;i<text_val.length;i++)
        		str+=text_val[i]+ " \t";
         }
         context.write(key, new Text (str));

     }
  } 