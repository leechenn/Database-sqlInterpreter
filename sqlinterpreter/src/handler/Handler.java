package handler;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operator.DuplicateEliminationOperator;
import operator.JoinOperator;
import operator.Operator;
import operator.ProjectOperator;
import operator.ScanOperator;
import operator.SelectOperator;
import operator.SortOperator;
import util.JoinExtract;

/**
 * @author Chen Li, QinXuan Pian
 * Handler class for parsing SQL and put reault to output dir
 */
public class Handler {
	
public static void init() {
//	get output path from Catalog and build a directory
	
	String outputPath = App.model.getOutputPath();
	new File(outputPath).mkdirs();
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
            	ScanOperator scanOperator = new ScanOperator(plainSelect);
            	Expression expression = plainSelect.getWhere();
            	SelectOperator selectOperator = new SelectOperator(scanOperator,plainSelect,expression);
            	ProjectOperator projectOperator = new ProjectOperator(selectOperator,plainSelect);
            	Operator operator = null;
            	 if(plainSelect.getDistinct() != null){
//            		 put projectOperator to SortOperator
                     operator = new SortOperator(projectOperator, plainSelect);
                     operator = new DuplicateEliminationOperator((SortOperator)operator);
                 }
                 else {
                 	operator = new SortOperator(projectOperator, plainSelect);
                 }
//            	 record the number of SQL so to store data into different output file
            	operator.dump(sqlCount);
            }

            Operator leftOp;
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
            	leftOp = new ScanOperator(plainSelect);
            	 if(App.model.getsingleTableExp()[0]!=null) {
            	 Expression singleTableExp = App.model.getsingleTableExp()[0].get(0);
            	leftOp = new SelectOperator((ScanOperator)leftOp,plainSelect,singleTableExp);
            	 }
            	 else {
            		 leftOp = new SelectOperator((ScanOperator)leftOp,plainSelect,null);
            	 }
                for(int i = 0; i < tableCount; ++i){
                    Operator opRight = new ScanOperator(plainSelect, i);
                    if(App.model.getsingleTableExp()[i+1]!=null) {
                    opRight = new SelectOperator((ScanOperator)opRight,plainSelect,App.model.getsingleTableExp()[i+1].get(0));
                    }
                    else {
                    	opRight = new SelectOperator((ScanOperator)opRight,plainSelect,null);
                    }
                    if(App.model.getJoinedTableExp()[i]!=null) {
                    leftOp = new JoinOperator(leftOp, opRight, plainSelect,App.model.getJoinedTableExp()[i].get(0));
                    }
                    else {
                    	leftOp = new JoinOperator(leftOp, opRight, plainSelect,null);
                    }
                    
                }
                ProjectOperator projectOperator = new ProjectOperator(leftOp,plainSelect);
                Operator operator = null;
                if(plainSelect.getDistinct() != null){
                    operator = new SortOperator(projectOperator, plainSelect);
                    operator = new DuplicateEliminationOperator((SortOperator)operator);
                }
                else {
                	operator = new SortOperator(projectOperator, plainSelect);
                }
                operator.dump(sqlCount);
                
            	
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
