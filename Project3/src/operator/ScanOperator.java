package operator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Map;

import entity.Tuple;
import handler.App;
import net.sf.jsqlparser.statement.select.PlainSelect;
import util.TupleReader;


/**
 * ScanOperator class 
 * @author Chen Li, QinXuan Pian
 */

public class ScanOperator extends Operator{
	
	private File file;
	private BufferedReader br = null;
	private Map<String, Integer> curSchema;
	String tableScanned;
	TupleReader tr;

	/**
	 * ScanOperator constructor for SQL without joined tables
	 */
	public ScanOperator(PlainSelect plainSelect) {
		
		tableScanned = null;
		tableScanned = plainSelect.getFromItem().toString();// first table in from item
		
		String[] strs = tableScanned.split("\\s+");//if there is aliases
		tableScanned = strs[0];
		String aliasName = strs[strs.length-1];
		tr = new TupleReader(tableScanned);
		this.file = new File(App.model.getDataStoredPath(tableScanned));
		if(strs.length!=1) {
		App.model.setAliaMap(strs);
		App.model.setCurSchema(aliasName);  
		}
		else {
			App.model.iniCurSchema(tableScanned);
		}
//		iniread();
		curSchema = App.model.getCurSchema();
	}
	/**
	 * ScanOperator constructor for SQL joined tables
	 */
	public ScanOperator(PlainSelect plainSelect,int joinedTableIndex) {
		
		tableScanned = null;
		tableScanned = plainSelect.getJoins().get(joinedTableIndex).toString();
		String[] strs = tableScanned.split("\\s+");//if there is aliases
		tableScanned = strs[0];
		String aliasName = strs[strs.length-1];
		tr = new TupleReader(tableScanned);
		this.file = new File(App.model.getDataStoredPath(tableScanned));
		if(strs.length!=1) {
		App.model.setAliaMap(strs);
		App.model.setCurSchema(aliasName);
		}
		else {
			App.model.iniCurSchema(tableScanned);
		}
//		iniread();
		curSchema = App.model.getCurSchema();
		
	}

	@Override
	public Tuple getNextTuple() {
		Tuple tuple = null;
		
			long[] newTupleData = tr.nextTuple();
//			System.out.println(Arrays.toString(newTupleData));
//			String str = br.readLine();
			if(newTupleData!=null) {
//			 System.out.println(Arrays.toString(newTupleData));
			   String dataString = Arrays.toString(newTupleData).replaceAll(" ", "");
			   int len = dataString.length();
			   tuple = new Tuple(dataString.substring(1, len-1));
			
			}
			
		
		
		return tuple;
		
	}

	@Override
	public void reset() {	
		tr.close();
		tr = new TupleReader(tableScanned);
	}


	public Map<String,Integer> getSchema() {
		
		return this.curSchema;
	}
	public void iniread() {
//		tr.close();
		tr = new TupleReader(tableScanned);
//			tr = new TupleReader(this.tableScanned);
//			Reader read = new FileReader(this.file);
//			br = new BufferedReader(read);
		
		
		
	}
	

}