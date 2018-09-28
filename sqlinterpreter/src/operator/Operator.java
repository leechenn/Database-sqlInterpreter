package operator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import entity.Tuple;
import handler.App;

public abstract class Operator {
	
	public abstract Tuple getNextTuple();
	
	
	public abstract void reset(); 	
	
	
	public void dump(int count) {
	
		        String outPutFile = App.model.getOutputPath();
		        BufferedWriter output;
		        try {
		            File file = new File(outPutFile+"/query"+count);
		            Writer write = new FileWriter(file);
		            StringBuilder sb = new StringBuilder();
		            output = new BufferedWriter(write);
		            Tuple tuple = getNextTuple();
		            while(tuple != null){
		                sb.append(tuple.toString());
		                sb.append("\n");
		                System.out.println(tuple);
		                tuple = getNextTuple();
		            }
		            output.write(sb.toString());
		            output.close();
		        } catch(IOException e) {
		            e.printStackTrace();
		        }
		        reset();
		    }
	
	
	

}
