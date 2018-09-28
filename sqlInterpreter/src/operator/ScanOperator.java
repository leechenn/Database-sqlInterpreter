package operator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import entity.Tuple;
import handler.App;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class ScanOperator extends Operator{
	//	private String TableScanned;
	private File file;
	private BufferedReader br = null;


	public ScanOperator(PlainSelect plainSelect) {
		String tableScanned = null;
		tableScanned = plainSelect.getFromItem().toString();
		String[] strs = tableScanned.split("\\s+");//if there is aliases
		String tableName = strs[0];
		String aliasName = strs[strs.length-1];
		this.file = new File(App.model.getDataStoredPath(tableName));
		if(strs.length!=1) {
		App.model.setAliaMap(strs);
		App.model.setCurSchema(aliasName);
		}
		else {
			App.model.iniCurSchema(tableName);
		}
		iniread();
		
		
		
		

	}

	@Override
	public Tuple getNextTuple() {
		Tuple tuple = null;
		try {
			String str = br.readLine();
			if(str!=null) {
				tuple = new Tuple(str);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return tuple;
		
	}

	@Override
	public void reset() {	
		iniread();
	}

//	@Override
//	public void dump() {	
//	}
	public void iniread() {
		try {
			Reader read = new FileReader(this.file);
			br = new BufferedReader(read);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}