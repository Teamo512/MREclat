package mrEclat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends Reducer<KeyWritable, ValueWritable, KeyWritable, ValueWritable>{
	
	public int minSupport;
	private int firstLevelsize;
	
	protected void setup(Context context) throws IOException, InterruptedException{
		minSupport = context.getConfiguration().getInt("minSupport", 0);
		
	}
	

	public void reduce( KeyWritable key, Iterable<ValueWritable> values,Context context)throws IOException, InterruptedException {

		ArrayList<ValueWritable> sibling = new ArrayList<ValueWritable>();
		for (ValueWritable v : values) {
			ValueWritable val = new ValueWritable(v);//这一句必须加，不然每次添加的v都是一样的，因为每次改变的都是同一对象，且每次
			sibling.add(val);                         //添加都是指向该对象，所以多次添加，都会变成指向同一变量多次罢了
		}
		Collections.sort(sibling);
		firstLevelsize = sibling.get(0).itemset.length;
		mining(sibling, context);
	}
	
	public void mining(ArrayList<ValueWritable> sibling, Context context) throws IOException, InterruptedException{
		
		
		//KeyWritable totalKWritable = new KeyWritable();
		//ValueWritable childWritable = null;
		for(int i=0; i<sibling.size(); i++){
				
				int[] itemset_i = sibling.get(i).itemset;
				int[] tidset_i = sibling.get(i).tidset;
				
				//ValueWritable vWritable = new ValueWritable(itemset_i, tidset_i);
				if(sibling.get(i).itemset.length != firstLevelsize) {
					//context.write(totalKWritable, vWritable);  //不进行输出，仅测试时间！！！
					context.getCounter(MREclatCounter.TotalNum).increment(1);
				}
				
				
				ArrayList<ValueWritable> children = new ArrayList<ValueWritable>();
				
				for(int j=i+1; j<sibling.size(); j++){
					int[] itemset_j = sibling.get(j).itemset;
					int[] tidset_j = sibling.get(j).tidset; 

					int[] childItemset = combineList(itemset_i, itemset_j);
					int[] tidset_ij = intersection(tidset_i, tidset_j);
					
					
					if(tidset_ij.length >= minSupport){
							children.add(new ValueWritable(childItemset, tidset_ij));
					}
				}
				
				sibling.set(i, null);  
				
				Collections.sort(children); 

				if(children.size() >= 1){
					mining(children,context);
				}
			}
		}
	
	protected static int[] combineList(int[] array1, int[] array2){
		List<Integer> list = new ArrayList<Integer>(array1.length+array2.length);
		int i=0, j=0;
		while(i<array1.length && j<array2.length){
			int compare = array1[i] - array2[j];
			if(compare < 0){
				list.add(array1[i]);
				i++;
			}else if(compare == 0){
				list.add(array1[i]);
				i++;
				j++;
			}else{
				list.add(array2[j]);
				j++;
			}
		}
		while(i < array1.length)
			list.add(array1[i++]);
		while(j < array2.length)
			list.add(array2[j++]);
		return toIntArray(list);
	}
	
	protected static int[] intersection(int[] array1, int[] array2){
		List<Integer> list = new ArrayList<Integer>(array1.length);
		int i=0,j=0;
		while(i<array1.length && j<array2.length){
			if(array1[i] == array2[j]){
				list.add(array1[i]);
				i++;
				j++;
			}else if(array1[i] < array2[j])
				i++;
			else
				j++;
		}
		return toIntArray(list);		
	}
	
	public static int[] toIntArray(List<Integer> list) {
	    int[] intArray = new int[list.size()];
	    int ix = 0;
	    for (Integer i : list) {
	      intArray[ix++] = i;
	    }
	    return intArray;
	  }
	
}
