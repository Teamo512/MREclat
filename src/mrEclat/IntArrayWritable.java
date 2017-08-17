package mrEclat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class IntArrayWritable implements WritableComparable<IntArrayWritable>{
	
	private int[] value;
	
	public IntArrayWritable(){
		
	}

	public IntArrayWritable(int[] value){
		this.value = value;
	}
	

	
	public IntArrayWritable(IntArrayWritable iaw){
		this.value = iaw.value;
	}
	
	public int[] get(){
		return value;
	}
	
	
	@Override
	public int hashCode() {
		
		return Arrays.hashCode(value);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntArrayWritable other = (IntArrayWritable) obj;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(value.length);
		for (int aValue : value) 
			out.writeInt(aValue);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		value = new int[size]; 
		for(int i=0; i<size; i++)
			value[i] = in.readInt();
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("I[");
		for(int i=0; i<value.length; i++){
			sb.append(value[i]);
			if(i != value.length-1){
				sb.append(",");
			}	
		}
		sb.append("]");
		return sb.toString();
	}

	@Override
	public int compareTo(IntArrayWritable o) {
		int[] list1 = this.value;
		int[] list2 = o.value;
		if(list1.length == list2.length){
			for(int i=0; i<list1.length; i++){
				if(list1[i] != list2[i])
					return list1[i] - list2[i];
			}
			return 0;
		}else{
			int i=0;
			for(i=0; i<list1.length&&i<list2.length; i++){
				if(list1[i] != list2[i])
					return list1[i] - list2[i];
			}
			if(i==list1.length){
				return 0 - list2[i];
			}else{
				return list1[i];
			}
		}
	}
	

}
