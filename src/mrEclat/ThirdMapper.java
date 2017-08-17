package mrEclat;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdMapper extends Mapper<IntArrayWritable,IntArrayWritable,KeyWritable,ValueWritable>{
	HashMap<Integer, Integer> hashmap;
	
	protected void setup(Context context) throws IOException, InterruptedException{
		hashmap = new HashMap<Integer, Integer>();
		URI[] paths = null; 
		paths = context.getCacheFiles();
		if (paths == null || paths.length <= 0) {
			System.out.println("No DistributedCache keywords File!");
			System.exit(1);
		}
		SequenceFile.Reader reader = null;
		try{			
			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			for (URI path : paths) {
				reader = new SequenceFile.Reader(context.getConfiguration(), Reader.file(new Path(path)));
				while(reader.next(key, value)) {
					hashmap.put(key.get(), value.get());
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(reader);
		}
	}
	
	public void map(IntArrayWritable key, IntArrayWritable value, Context context)throws IOException, InterruptedException{
		
		int prefix = key.get()[0];
		if(hashmap.containsKey(prefix)) {
			KeyWritable kw = new KeyWritable(prefix, hashmap.get(prefix));
			ValueWritable vw = new ValueWritable(key.get(), value.get());
		
			context.write(kw, vw);
		}
		
		
	}

}
