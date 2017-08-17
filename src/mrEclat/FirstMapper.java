package mrEclat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

IntWritable one = new IntWritable(1);
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] line = value.toString().split(" ");
		for(int i=1; i<line.length; i++){
			context.write(new IntWritable(Integer.parseInt(line[i])), one);
		}
		
	}
}
