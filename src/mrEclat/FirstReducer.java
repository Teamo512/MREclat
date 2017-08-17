package mrEclat;

import static mrEclat.Driver.IT_PART;
import static mrEclat.Driver.PART;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FirstReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	private int minSupport;
	private MultipleOutputs<IntWritable, IntWritable> mos;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		minSupport = context.getConfiguration().getInt("minSupport", 0);
		mos = new MultipleOutputs<IntWritable, IntWritable>(context);

	}
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int sum = 0;
		
		for(IntWritable value : values){
			sum += value.get();
		}
		if(sum >= minSupport){
			//context.write(key, new IntWritable(sum));
			mos.write(key, new IntWritable(sum), IT_PART + "/" + PART);
			context.getCounter(MREclatCounter.TotalNum).increment(1);
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
	    mos.close();
	}

}
