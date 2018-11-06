package handler;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import entity.Tuple;
import logicaloperators.LogicalDuplicateEliminationOperator;
import logicaloperators.LogicalJoinOperator;
import logicaloperators.LogicalOperator;
import logicaloperators.LogicalProjectOperator;
import logicaloperators.LogicalScanOperator;
import logicaloperators.LogicalSelectOperator;
import logicaloperators.LogicalSortOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operator.IndexScanOperator;
import operator.Operator;
import util.JoinExtract;
import util.PhysicalPlanBuilder;

/**
 * @author Chen Li, QinXuan Pian
 * Handler class for parsing SQL and put reault to output dir
 */
public class Handler {
	public static void main(String[] args) {
		String[] strArray  = {"samples-2 2/interpreter_config_file.txt"};
		App.main(strArray);
//		IndexScanOperator indexScanOperator = new IndexScanOperator("Boats", "E", 6500, null,false);
//		System.out.println(indexScanOperator.getNextTuple());
//		System.out.println(indexScanOperator.getNextTuple());
//		System.out.println(indexScanOperator.getNextTuple());
//		Tuple tuple = indexScanOperator.getNextTuple();
//		while(tuple!=null) {
//			System.out.println(tuple);
//			tuple = indexScanOperator.getNextTuple();
//		}
		
		
//		TupleReader tr = new TupleReader("Sailors");
//		System.out.println(tr.readTuple(5, 81));
		
	}
public static void init() {
//	get output path from Catalog and build a directory
	String outputPath = App.model.getOutputPath();
	new File(outputPath).mkdirs();
	new File(App.model.temDir).mkdirs();
}
public static void parseSql() {
	try {
        String sqlFile = App.model.getSqlPath();
        CCJSqlParser parser = new CCJSqlParser(new FileReader(sqlFile));
        Statement statement;
        int sqlCount = 1;
        while ((statement = parser.Statement()) != null) { 
            Select select = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
            try {
            if(plainSelect.getJoins()==null) {
            	LogicalScanOperator logicalScanOperator = new LogicalScanOperator(plainSelect);
            	Expression expression = plainSelect.getWhere();
            	LogicalSelectOperator logicalSelectOperator = new LogicalSelectOperator(logicalScanOperator,plainSelect,expression);
            	LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(logicalSelectOperator,plainSelect);
            	Operator operator = null;
            	LogicalOperator logicalOperator = logicalProjectOperator;
            	 if(plainSelect.getDistinct() != null){
                 	logicalOperator = new LogicalSortOperator(logicalOperator, plainSelect);
                     logicalOperator = new LogicalDuplicateEliminationOperator(logicalOperator);
                 }else {
                 	if(plainSelect.getOrderByElements()!=null) {
                 		logicalOperator = new LogicalSortOperator(logicalOperator, plainSelect);
                 	}
                 }


                PhysicalPlanBuilder ppb = new PhysicalPlanBuilder();
             	logicalOperator.accept(ppb);
           	    operator  = ppb.getOp();
            	 operator  = ppb.getOp();	 
            	 long startTime = System.currentTimeMillis();
            	 operator.dump(sqlCount);
            	 long elapsedTime = System.currentTimeMillis() - startTime;
      		     System.out.println("Elapsed Time is: " + elapsedTime);
            	
            }

            Operator leftOp;
            LogicalOperator leftLogicalOp;
            if(plainSelect.getJoins()!=null) {
            	List list = plainSelect.getJoins();
            	int size = list.size();
            	App.model.iniAllTableList();
            	List<String> tableList = App.model.getAllTableList();
            	String firstTable = plainSelect.getFromItem().toString();
        		String[] strs = firstTable.split("\\s+");//if there is aliases
        		String tableName = strs[0];
        		String aliasName = strs[strs.length-1];
        		if(strs.length!=1) {
        		tableList.add(aliasName);
        		}
        		else {
        			tableList.add(tableName);
        		}
            	//first table
        		App.model.iniJoinedTableList();
            	List<String> joinedTableList = App.model.getJoinedTableList();
            	for(int i = 0;i<size;i++) {
            		String tableScanned = plainSelect.getJoins().get(i).toString();
            		String[] joinTableStrs = tableScanned.split("\\s+");//if there is aliases
            		tableName = joinTableStrs[0];
            		aliasName = joinTableStrs[joinTableStrs.length-1];
            		if(strs.length!=1) {
            		tableList.add(aliasName);
            		joinedTableList.add(aliasName);
            		}
            		else {
            			tableList.add(tableName);
            			joinedTableList.add(tableName);
            		}
            	}
            	App.model.iniSingleTableExp(App.model.getAllTableList().size());
            	App.model.iniJoinedTableExp(App.model.getJoinedTableList().size());
                App.model.iniAllExp();
            	App.model.iniSingleTableExp(tableList.size());
            	App.model.iniJoinedTableExp(tableList.size()-1);
            	JoinExtract joinExtractVisitor = new JoinExtract();
            	if(plainSelect.getWhere()!=null) {
            	plainSelect.getWhere().accept(joinExtractVisitor);
            	}

            	int tableCount = plainSelect.getJoins().size();
            	leftLogicalOp = new LogicalScanOperator(plainSelect);
            	 if(App.model.getsingleTableExp()[0]!=null) {
            	 Expression singleTableExp = App.model.getsingleTableExp()[0].get(0);
            	 leftLogicalOp = new LogicalSelectOperator(leftLogicalOp,plainSelect,singleTableExp);

            	 }
            	 else {
            		 leftLogicalOp = new LogicalSelectOperator(leftLogicalOp,plainSelect,null);

            	 }
                for(int i = 0; i < tableCount; ++i){

                    LogicalOperator LogicalOperatorRight = new LogicalScanOperator(plainSelect, i);
                    if(App.model.getsingleTableExp()[i+1]!=null) {
 

                    LogicalOperatorRight = new LogicalSelectOperator(LogicalOperatorRight,plainSelect,App.model.getsingleTableExp()[i+1].get(0));
                    }
                    else {

                    	LogicalOperatorRight = new LogicalSelectOperator(LogicalOperatorRight,plainSelect,null);
                    }
                    if(App.model.getJoinedTableExp()[i]!=null) {

                    leftLogicalOp = new LogicalJoinOperator(leftLogicalOp, LogicalOperatorRight, plainSelect,App.model.getJoinedTableExp()[i].get(0));
                    }
                    else {

                    	leftLogicalOp = new LogicalJoinOperator(leftLogicalOp, LogicalOperatorRight, plainSelect,null);
                    }
                    
                }

                LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(leftLogicalOp,plainSelect);
                Operator operator = null;
                LogicalOperator logicalOperator = logicalProjectOperator;
              
                if(plainSelect.getDistinct() != null){
                	logicalOperator = new LogicalSortOperator(logicalOperator, plainSelect);
                    logicalOperator = new LogicalDuplicateEliminationOperator(logicalOperator);
                }else {
                	if(plainSelect.getOrderByElements()!=null) {
                		logicalOperator = new LogicalSortOperator(logicalOperator, plainSelect);
                	}
                }

              
                PhysicalPlanBuilder ppb = new PhysicalPlanBuilder();
             	logicalOperator.accept(ppb);
           	    operator  = ppb.getOp();
           	    long startTime = System.currentTimeMillis(); 
           	    operator.dump(sqlCount);
           	    long elapsedTime = System.currentTimeMillis() - startTime;
     		    System.out.println("Elapsed Time is: " + elapsedTime);
           	  
                
            	
            }
            }

            finally{sqlCount = sqlCount + 1;}
        }
    } catch (Exception e) {
        System.err.println("Exception occurred during parsing");
        e.printStackTrace();
    }
}
}
