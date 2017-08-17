package mrEclat;

public class ItemWeight {
	
	public int item;
	public double weigth;
	
	
	public ItemWeight() {
		
	}
	
	public ItemWeight(ItemWeight w) {
		this.item = w.item;
		this.weigth = w.weigth;
	}
	
	public ItemWeight(int item, double weigth) {
		this.item = item;
		this.weigth = weigth;
	}
	public int getItem() {
		return item;
	}
	public void setItem(int item) {
		this.item = item;
	}
	public double getWeight() {
		return weigth;
	}
	public void setWeight(double weigth) {
		this.weigth = weigth;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + item;
		long temp;
		temp = Double.doubleToLongBits(weigth);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ItemWeight other = (ItemWeight) obj;
		if (item != other.item)
			return false;
		if (Double.doubleToLongBits(weigth) != Double.doubleToLongBits(other.weigth))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "ItemWeigth [item=" + item + ", weigth=" + weigth + "]";
	}
	
	

}
