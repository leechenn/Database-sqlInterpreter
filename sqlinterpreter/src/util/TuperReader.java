package util;

import entity.Tuple;

public interface TuperReader {
	
	public Tuple readNext();
	
	public void close();
	
	public void reset();

}
