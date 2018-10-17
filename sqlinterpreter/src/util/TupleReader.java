package util;

import entity.Tuple;

public interface TupleReader {
	
	public void fillPage();
	
	public Tuple readNext();
	
	public void close();
	
	public void reset();

}
