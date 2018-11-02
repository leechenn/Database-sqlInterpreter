package operator;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import entity.Tuple;
import handler.App;
import util.TupleReader;
import util.TupleWriter;


/**
 * Abstract class for different kinds of operator
 * @author Chen Li, QinXuan Pian
 */
public abstract class Operator {

	private Map<String, Integer> curSchema;
	int totalCount = 0;
	
	/**
	 * input results of SQL to corresponding outputDir, the name of the output file is query+SQLN
	 *
	 */
	public void dump(int count) {
		
		String outPutFile = App.model.getOutputPath();
		TupleWriter tw = new TupleWriter(outPutFile+"/query"+count);
		

		try {
			totalCount = 0;
			Tuple tuple = getNextTuple();
			while(tuple != null){				
				totalCount++;	
				tw.writeTuple(tuple);
				tuple = getNextTuple();					
			}

		} catch(IOException e) {
			e.printStackTrace();
		}
		tw.close();
		System.out.println("TupleCount:"+totalCount);
		ExternalSortOperator.deleteDirectory(new File(App.model.temDir));
		new File(App.model.temDir).mkdirs();
		
	}
	public Map<String,Integer> getSchema(){
		return this.curSchema;
	};
	
	public abstract Tuple getNextTuple();
	
	public abstract void reset(); 	






}
