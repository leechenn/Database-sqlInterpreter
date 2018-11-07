package bPlusTree;

import java.util.Comparator;
import java.util.List;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class DataEntry {
	
	private int key;
	private List<Rid> ridList;

	public DataEntry(int key,List<Rid> ridList) {
		this.key = key;
		this.ridList = ridList;
	}
	public int getKey() {
		return this.key;
	}
	public void addRid(Rid rid) {
		this.ridList.add(rid);
	}
	public List<Rid> getRidList() {
		return this.ridList;
	}
	
	@Override 
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("<[" + key +":");
		for (Rid rid : ridList) {
			sb.append(rid.toString());
		}
		sb.append("]>");
		return sb.toString();
	}
	
	/**
	 * @author Chen Li, QinXuan Pian
	 * Data Entry Comparator for sorting data entry
	 */
	public static class dataEntryCompare implements Comparator<DataEntry>{

		
		@Override
		public int compare(DataEntry o1, DataEntry o2) {
			// TODO Auto-generated method stub
			if(o1.key>o2.key) {
				return 1;
			}
			if(o1.key<o2.key) {
				return -1;
			}
			return 0;
		}
		
	}
}

