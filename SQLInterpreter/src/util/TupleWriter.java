package util;

import entity.Tuple;

public interface TupleWriter {
	
	public void fillBuffer(Tuple input);
	
	public void writeFile();

}
