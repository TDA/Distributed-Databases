import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
public class EquiJoinReducer extends
        Reducer<IntWritable,Text, IntWritable,Text> {
    protected void reduce(IntWritable key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
    	ArrayList<String> rTuples = new ArrayList<String>();
		ArrayList<String> sTuples = new ArrayList<String>();
		
		for (Text val : values) {
			String tpl = val.toString();
			if(tpl.charAt(0) == 'R' || tpl.charAt(0) == 'r')
				rTuples.add(tpl.substring(2));
			else
				sTuples.add(tpl.substring(2));
		}
		for(String tpl1:rTuples){
			for(String tpl2:sTuples){
				context.write(key, new Text(tpl1+tpl2));
			}
			
		}
     }
  } 