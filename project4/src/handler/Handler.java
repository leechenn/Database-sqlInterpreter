package handler;

import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import logicaloperators.LogicalDuplicateEliminationOperator;
import logicaloperators.LogicalJoinOperator;
import logicaloperators.LogicalMultiJoinOperator;
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
import operator.Operator;
import util.Element;
import util.JoinExtract;
import util.PhysicalPlanBuilder;
import util.Tool;
import util.UnionFindVisitor;

/**
 * @author Chen Li, QinXuan Pian
 * Handler class for parsing SQL and put reault to output dir
 */
public class Handler {
	public static void main(String[] args) {
		String[] strArray  = {"samples/interpreter_config_file.txt"};
		App.main(strArray);


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
				File logicalPlanFile = new File(App.model.getOutputPath()+"/query"+sqlCount+"_logicalplan");
				File physicalPlanFile = new File(App.model.getOutputPath()+"/query"+sqlCount+"_physicalplan");
				PrintStream logicalPlanStream = new PrintStream(logicalPlanFile);
				PrintStream physicalPlanStream = new PrintStream(physicalPlanFile);		
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
						
						UnionFindVisitor ufVisitor = new UnionFindVisitor(tableList);//tableList to store expression info
						
						if(plainSelect.getWhere()!=null) {
							plainSelect.getWhere().accept(joinExtractVisitor);					
							plainSelect.getWhere().accept(ufVisitor);
							System.out.println("ElementList:"+ufVisitor.getUf().getElementList());
							System.out.println("residual:"+ufVisitor.getResidualExp());
						}
						
						HashMap<String, List<Expression>> selectionMap = ufVisitor.getSelectionMap();
						
						for (String attr : ufVisitor.getUf().getUnionMap().keySet()) {
							Element ufe = ufVisitor.getUf().findElement(attr);
							String tab = attr.split("\\.")[0];
							String col = attr.split("\\.")[1];
							List<Expression> lst = selectionMap.get(tab);
							Integer eq = ufe.getEquality();
							Integer lower = ufe.getLowerB();
							Integer upper = ufe.getUpperB();				
							if (eq != null)
								lst.add(Tool.createCondition(
										tab, col, eq, true, false));
							else {
								if (lower != null)
									lst.add(Tool.createCondition(
											tab, col, lower, false, true));
								if (upper != null)
									lst.add(Tool.createCondition(
											tab, col, upper, false, false));
							}
						}
						HashMap<String,Expression> finalSelectionCondition = new HashMap<String,Expression>();
						for(String tab:App.model.getAllTableList()) {
							finalSelectionCondition.put(tab, Tool.genAnds(selectionMap.get(tab)));
						}
						List<Expression> residualJoinCondition = ufVisitor.getResidualJoinExp();
						Expression finalResidualCondition = Tool.genAnds(residualJoinCondition);
						System.out.println("residualJoinCondition:"+finalResidualCondition );
						System.out.println("SelectionCondition:"+finalSelectionCondition);
						
						//for multiJoin
						
						List<LogicalOperator> tables = new ArrayList<>();
						LogicalOperator tmp =  new LogicalScanOperator(plainSelect);
						if(finalSelectionCondition.get(App.model.getAllTableList().get(0))!=null) {
							tmp = new LogicalSelectOperator(tmp,plainSelect, finalSelectionCondition.get(App.model.getAllTableList().get(0)));
						}
						tables.add(tmp);
						

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
							tmp =  new LogicalScanOperator(plainSelect,i);
							if(finalSelectionCondition.get(App.model.getAllTableList().get(i+1))!=null) {
								tmp = new LogicalSelectOperator(tmp,plainSelect, finalSelectionCondition.get(App.model.getAllTableList().get(i+1)));
							}
							tables.add(tmp);
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
						LogicalOperator root;
						root = new LogicalMultiJoinOperator(App.model.getAllTableList(),tables,finalResidualCondition, ufVisitor.getUf());
						root = new LogicalProjectOperator(root, plainSelect);
						if(plainSelect.getDistinct() != null){
							root = new LogicalSortOperator(root, plainSelect);
							root = new LogicalDuplicateEliminationOperator(root);
						}else {
							if(plainSelect.getOrderByElements()!=null) {
								root = new LogicalSortOperator(root, plainSelect);
							}
						}
						
						root.printTree(System.out, 0);
						root.printTree(logicalPlanStream, 0);
						logicalPlanStream.close();
						
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
