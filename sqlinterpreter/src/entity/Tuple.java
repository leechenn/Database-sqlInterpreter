package entity;

import java.util.Arrays;


/**
 * Tuple class  
 * Record data in each tuple
 */
public class Tuple {
	private int[] data; //data in tuple 
	private String dataString;//String format data

	public Tuple(String s) {
		this.dataString = s;
		String[] sData = s.split(",");
		data = new int[sData.length];
		for(int i = 0; i < sData.length; i++){
			data[i] = Integer.parseInt(sData[i]);
		}
	}
	/**
	 * verify if two tuple are equal according to data in tuple
	 */
	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}
		if (object == null || !(object instanceof Tuple)) {
			return false;
		}
		Tuple tuple = (Tuple) object;

		return Arrays.equals(data, tuple.data);
	}
	/**
	 * get data as an Integer array
	 */
	public int[] getData() {
		return this.data;
	}

	@Override
	public String toString() {
		return dataString;
	}

}
