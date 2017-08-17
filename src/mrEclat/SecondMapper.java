package mrEclat;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<Object, Text, IntArrayWritable, IntWritable>{
	private Map<Integer, Integer> allItemsMap; 
	private int sortItemsMethod;
	
	protected void setup(Context context) throws IOException, InterruptedException{
		allItemsMap = new HashMap<Integer, Integer>();
		sortItemsMethod = context.getConfiguration().getInt("SortItemsMethod", 0);
		
		URI[] URIs = context.getCacheFiles();
		if (URIs == null || URIs.length <= 0) {
			System.out.println("No DistributedCache keywords File!");
			System.exit(1);
		}
		
		SequenceFile.Reader reader = null;
		try{
			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			for (URI uri : URIs) {
				reader = new SequenceFile.Reader(context.getConfiguration(), Reader.file(new Path(uri)));
				while (reader.next(key, value)) {
					allItemsMap.put(key.get(), value.get());
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		
		if(allItemsMap != null && sortItemsMethod != 2) {	
			//sort frequent items by support ascending order,
			List<Entry<Integer,Integer>> list = new ArrayList<Entry<Integer,Integer>>(allItemsMap.entrySet());
			
			list.sort(new Comparator<Entry<Integer, Integer>>() {
				@Override
				public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
					return Objects.equals(o1.getValue(), o2.getValue()) ? (o1.getKey() - o2.getKey()) : (o1.getValue() - o2.getValue());
				}
			});
			if(sortItemsMethod == 1){
				//items are reversed and sorted by support descending order
				list.sort(new Comparator<Entry<Integer, Integer>>() {
					@Override
					public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
						return Objects.equals(o1.getValue(), o2.getValue()) ? (o2.getKey() - o1.getKey()) : (o1.getValue() - o2.getValue());
					}
				});
			}
			int i=0;
			for(Entry<Integer,Integer> entry : list){
				allItemsMap.put(entry.getKey(), i++);
			}
			list = null;
		}
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] line = value.toString().split(" ");
		int tid = Integer.parseInt(line[0]);
		ArrayList<Integer> list = new ArrayList<Integer>(line.length);
		int item;
		for(int i=1; i<line.length; i++){
			item = Integer.parseInt(line[i]);
			if(allItemsMap.containsKey(item))
				list.add(allItemsMap.get(item));
		}
		Collections.sort(list);
		int oneItem;
		for(int i=0; i<list.size(); i++){
			oneItem = list.get(i);
			for(int j=i+1; j<list.size(); j++){
				context.write(new IntArrayWritable(new int[] {oneItem, list.get(j)}), new IntWritable(tid));
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		super.cleanup(context);
		allItemsMap = null;
	}

}