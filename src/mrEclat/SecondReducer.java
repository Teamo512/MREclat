package mrEclat;

import static mrEclat.Driver.IT_PART;
import static mrEclat.Driver.PART;
import static mrEclat.Driver.TOTAL_PART;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SecondReducer extends Reducer<IntArrayWritable, IntWritable, IntArrayWritable, Writable>{
	
	private MultipleOutputs<IntArrayWritable, Writable> mos;
	private int minSupport;
	
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<IntArrayWritable, Writable>(context);
		minSupport = context.getConfiguration().getInt("minSupport", 0);
		
	}
	
	public void reduce(IntArrayWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		ArrayList<Integer> tidset = new ArrayList<Integer>();
		for(IntWritable v : values){
			tidset.add(v.get());
		}
		try{
			if(tidset.size() >= minSupport){
				Collections.sort(tidset);
				int[] tidArray = Tools.toIntArray(tidset);
				int[] itemset = key.get();
				//Writable valuewritavle = new IntArrayWritable(tidArray);
				mos.write(new IntArrayWritable(itemset), new IntArrayWritable(tidArray), IT_PART + "/" + PART);
				    
				mos.write(new IntArrayWritable(itemset), new IntArrayWritable(new int[]{tidArray.length}), TOTAL_PART + "/" + PART);			 
				context.getCounter(MREclatCounter.TotalNum).increment(1);
				
			}	
		}catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
	    mos.close();
	}
}
