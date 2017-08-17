package mrEclat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ValueWritable implements Writable, Comparable<ValueWritable>{
	
	public int[] itemset;
	public int[] tidset;
	
	public ValueWritable() {
		
	}
	
	public ValueWritable(int[] itemset, int[] tidset) {
		this.itemset = itemset;
		this.tidset = tidset;
	}
	
	public ValueWritable(ValueWritable vw) {
		this.itemset = vw.itemset;
		this.tidset = vw.tidset;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		int itemset_length = itemset.length;
		int tidset_length = tidset.length;
		out.writeInt(itemset_length);
		out.writeInt(tidset_length);
		for(int item : itemset) {
			out.writeInt(item);
		}
		for(int tid : tidset) {
			out.writeInt(tid);
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int itemset_length = in.readInt();
		int tidset_length = in.readInt();
		itemset = new int[itemset_length];
		tidset = new int[tidset_length];
		for(int i=0; i<itemset_length; i++) {
			itemset[i] = in.readInt();
		}
		for(int i=0; i<tidset_length; i++) {
			tidset[i] = in.readInt();
		}
		
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for(int item : itemset) {
			sb.append(item + " ");
		}
		sb.append("]" + tidset.length);
		return sb.toString();
		
	}

	@Override
	public int compareTo(ValueWritable o) {
		int[] array1 = this.itemset;
		int[] array2 = o.itemset;
		
		if(array1.length==array2.length){
			for(int i=0; i<array1.length; i++){
				if(array1[i] == array2[i]){
					continue;
				}else
					return array1[i] - array2[i];
			}
			return 0;
		}else{
			int i=0;
			for(i=0; i<array1.length&&i<array2.length; i++){
				if(array1[i] == array2[i]){
					continue;
				}else
					return array1[i] - array2[i];
			}
			if(i==array1.length){
				return 0 - array2[i];
			}else{
				return array1[i];
			}
		}
	}

}
