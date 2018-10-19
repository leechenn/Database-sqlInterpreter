package operator;

//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.Writer;
import java.util.Map;
import entity.Tuple;
import handler.App;
import util.BinaryTupleWriter;


/**
 * Abstract class for different kinds of operator
 * @author Chen Li, Qinxuan Pian
 */
public abstract class Operator {

	private Map<String, Integer> curSchema;
	private BinaryTupleWriter btw;
	
	/**
	 * input results of SQL to corresponding outputDir, the name of the output file is query+SQLN
	 *
	 */
	public void dump(int count) {
		String outPutFile = App.model.getOutputPath();
//		BufferedWriter output;
//		try {
//			File file = new File(outPutFile+"/query"+count);
//			Writer write = new FileWriter(file);
//			StringBuilder sb = new StringBuilder();
//			output = new BufferedWriter(write);
//			Tuple tuple = getNextTuple();
//			while(tuple != null){
//				sb.append(tuple.toString());
//				sb.append("\n");
//				tuple = getNextTuple();
//			}
//			output.write(sb.toString());
//			output.close();
//		} catch(IOException e) {
//			e.printStackTrace();
//		}
		
		btw = new BinaryTupleWriter(outPutFile+"/query"+count);
		Tuple tuple = getNextTuple();
		while (tuple!=null) {
			btw.fillBuffer(tuple);
			tuple = getNextTuple();
		}
		btw.finishWriting();
	}
	public Map<String,Integer> getSchema(){
		return this.curSchema;
	};
	
	public abstract Tuple getNextTuple();
	
	public abstract void reset(); 	






}
