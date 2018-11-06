package operator;

import java.io.File;
import java.util.Map;

import bPlusTree.Rid;
import bPlusTree.TreeDeserializer;
import entity.Tuple;
import handler.App;
import net.sf.jsqlparser.statement.select.PlainSelect;
import util.TupleReader;

public class IndexScanOperator extends Operator {
private Integer lowKey;
private Integer highKey;
private boolean isClustered;
private String table;
private String indexKey;
private Rid rid;
private TreeDeserializer td;
private TupleReader tr;
private String tableScanned;
private Map<String, Integer> curSchema;

public IndexScanOperator(PlainSelect plainSelect ,String table, String indexKey, Integer lowKey, Integer highKey, boolean isClustered) {
	
	
	tableScanned = null;
	tableScanned = plainSelect.getFromItem().toString();// first table in from item
	
	String[] strs = tableScanned.split("\\s+");//if there is aliases
	tableScanned = strs[0];
	String aliasName = strs[strs.length-1];
	if(strs.length!=1) {
	App.model.setAliaMap(strs);
	App.model.setCurSchema(aliasName);  
	}
	else {
		App.model.iniCurSchema(tableScanned);
	}

	curSchema = App.model.getCurSchema();
	
	
	
	
	this.lowKey = lowKey;
	this.highKey = highKey;
	this.isClustered = isClustered;
	this.table = table;
	this.indexKey = indexKey;
    File indexFile = new File(App.model.getIndexDir()+"/"+this.table+"."+this.indexKey);
	this.td = new TreeDeserializer(indexFile,lowKey,highKey);
	this.tr = new TupleReader(table);
	td.findStartLeaf();
	this.rid = td.getNextRid();
	if(rid!=null&&isClustered) {
		tr.findFilePosition(rid.getPageId(), rid.getTupleId());
	}
	
}
public Tuple getNextTuple() {
	Tuple tuple = null;
	if(rid == null) {
	return tuple;
	}else {
		if(!isClustered) {
			tr.findFilePosition(rid.getPageId(), rid.getTupleId());
			tuple = tr.readNext();
		}else {
		tuple = tr.readNext();
		}
		rid = td.getNextRid();
//		System.out.println("rid:"+rid);
		return tuple;
	}
	
}
@Override
public void reset() {
	// TODO Auto-generated method stub
	File indexFile = new File(App.model.getIndexDir()+"/"+this.table+"."+this.indexKey);
	this.td = new TreeDeserializer(indexFile,lowKey,highKey);
	this.tr = new TupleReader(table);
	td.findStartLeaf();
	this.rid = td.getNextRid();
	if(rid!=null&&isClustered) {
		tr.findFilePosition(rid.getPageId(), rid.getTupleId());
	}
	
}
public Map<String,Integer> getSchema() {
	
	return this.curSchema;
}
}
