package bPlusTree;

import java.util.Comparator;

public class Rid {
	
private int pageId;
private int tupleId;

public Rid(int pageId, int tupleId) {
	this.pageId = pageId;
	this.tupleId = tupleId;
}

public int getPageId() {
	return pageId;
}

public void setPageId(int pageId) {
	this.pageId = pageId;
}

public int getTupleId() {
	return tupleId;
}

public void setTupleId(int tupleId) {
	this.tupleId = tupleId;
}
@Override
public String toString() {
	return "("+this.pageId+","+this.tupleId+")";
	
}
public static class ridCompare implements Comparator<Rid>{

	@Override
	public int compare(Rid o1, Rid o2) {
		// TODO Auto-generated method stub
		if(o1.pageId>o2.pageId) {
			return 1;
		}
		if(o1.pageId<o2.pageId) {
			return -1;
		}
		if(o1.tupleId>o2.tupleId) {
			return 1;
		}
		if(o1.tupleId<o2.tupleId) {
			return -1;
		}
		return 0;
	}

	
	
	
}
}
