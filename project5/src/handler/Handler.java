package handler;

import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import logicaloperators.LogicalDuplicateEliminationOperator;
import logicaloperators.LogicalJoinOperator;
import logicaloperators.LogicalMultiJoinOperator;
import logicaloperators.LogicalOperator;
import logicaloperators.LogicalProjectOperator;
import logicaloperators.LogicalScanOperator;
import logicaloperators.LogicalSelectOperator;
import logicaloperators.LogicalSortOperator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operator.Operator;
import util.Element;
import util.JoinExtract;
import util.JoinOrderDeterminator;
import util.PhysicalPlanBuilder;
import util.Tool;
import util.UnionFind;
import util.UnionFindVisitor;

/**
 * @author Chen Li, QinXuan Pian
 * Handler class for parsing SQL and put reault to output dir
 */
public class Handler {

	public static UnionFindVisitor ufVisitor;
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
				List<String> mapTableList = new ArrayList<String>();
				List list = plainSelect.getJoins();
				int size = 0;
				if(list!=null) {
					size = list.size();
				}
				App.model.iniAllTableList();
				List<String> tableList = App.model.getAllTableList();
				String firstTable = plainSelect.getFromItem().toString();
				String[] strs = firstTable.split("\\s+");//if there is aliases
				String tableName = strs[0];
				String aliasName = strs[strs.length-1];
				if(strs.length!=1) {
					tableList.add(aliasName);
					mapTableList.add(tableName);
					App.model.setAliaMap(strs);
					App.model.setCurSchema(aliasName);
				}
				else {
					tableList.add(tableName);
					mapTableList.add(tableName);
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
						mapTableList.add(tableName);
						App.model.setAliaMap(joinTableStrs);
						App.model.setCurSchema(aliasName);

					}
					else {
						tableList.add(tableName);
						joinedTableList.add(tableName);
						mapTableList.add(tableName);
					}
				}
				try {
					if(plainSelect.getJoins()==null) {
						ufVisitor = new UnionFindVisitor(tableList);//tableList to store expression info

						if(plainSelect.getWhere()!=null) {
							//							plainSelect.getWhere().accept(joinExtractVisitor);					
							plainSelect.getWhere().accept(ufVisitor);
							//							System.out.println("ElementList:"+ufVisitor.getUf().getElementList());
							//							System.out.println("residual:"+ufVisitor.getResidualExp());
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
						LogicalOperator tmp =  new LogicalScanOperator(plainSelect);
						if(finalSelectionCondition.get(App.model.getAllTableList().get(0))!=null) {
							tmp = new LogicalSelectOperator(tmp,plainSelect, finalSelectionCondition.get(App.model.getAllTableList().get(0)));
						}
						//						LogicalScanOperator logicalScanOperator = new LogicalScanOperator(plainSelect);
						//						Expression expression = plainSelect.getWhere();
						//						LogicalSelectOperator logicalSelectOperator = new LogicalSelectOperator(logicalScanOperator,plainSelect,expression);
						//						LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(logicalSelectOperator,plainSelect);
						Operator operator = null;
						//						LogicalOperator logicalOperator = logicalProjectOperator;
						LogicalOperator root = new LogicalProjectOperator(tmp, plainSelect);
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
						//						if(plainSelect.getDistinct() != null){
						//							logicalOperator = new LogicalSortOperator(logicalOperator, plainSelect);
						//							logicalOperator = new LogicalDuplicateEliminationOperator(logicalOperator);
						//						}else {
						//							if(plainSelect.getOrderByElements()!=null) {
						//								logicalOperator = new LogicalSortOperator(logicalOperator, plainSelect);
						//							}
						//						}


						PhysicalPlanBuilder ppb = new PhysicalPlanBuilder();
						root.accept(ppb);
						operator  = ppb.getOp();
						operator.printTree(System.out, 0);
						operator.printTree(physicalPlanStream, 0);
						physicalPlanStream.close();
						long startTime = System.currentTimeMillis();
						operator.dump(sqlCount);
						long elapsedTime = System.currentTimeMillis() - startTime;
						System.out.println("Elapsed Time is: " + elapsedTime);

					}

					Operator leftOp;
					LogicalOperator leftLogicalOp;
					if(plainSelect.getJoins()!=null) {

						App.model.iniSingleTableExp(App.model.getAllTableList().size());
						App.model.iniJoinedTableExp(App.model.getJoinedTableList().size());
						App.model.iniAllExp();
						App.model.iniSingleTableExp(tableList.size());
						App.model.iniJoinedTableExp(tableList.size()-1);
						JoinExtract joinExtractVisitor = new JoinExtract();
						Map<String,Integer> reductionMap = new HashMap<String,Integer>();
						for(String table:tableList) {
							reductionMap.put(table,App.model.getStats().getStatsMap().get(App.model.searchTableName(table)).getTupleNum());
						}
						ufVisitor = new UnionFindVisitor(tableList);//tableList to store expression info

						plainSelect.getWhere().accept(ufVisitor);
						JoinOrderDeterminator jd = new JoinOrderDeterminator(mapTableList, tableList, reductionMap, ufVisitor.getUf());

						List<Integer> orderList = jd.getOrder();
						//						System.out.println("order:"+orderList);

						HashMap<String, List<Expression>> selectionMap = ufVisitor.getSelectionMap();

						for (String attr : ufVisitor.getUf().getUnionMap().keySet()) {
							Element ufe = ufVisitor.getUf().findElement(attr);
							String tab = attr.split("\\.")[0];
							String col = attr.split("\\.")[1];
							List<Expression> lst = selectionMap.get(tab);
							Integer eq = ufe.getEquality();
							Integer lower = ufe.getLowerB();
							Integer upper = ufe.getUpperB();
							Integer tableLower = App.model.getStats().getStatsMap().get(App.model.searchTableName(tab)).getInfoMap().get(col)[0];
							Integer tableUpper = App.model.getStats().getStatsMap().get(App.model.searchTableName(tab)).getInfoMap().get(col)[1];
							long lBound2 = (lower==null) ? tableLower : Math.max(tableLower, lower);
							long uBound2 = (upper==null) ? tableUpper : Math.min(upper, tableUpper);
							float reFactor = (float)(uBound2-lBound2+1)/(tableUpper-tableLower+1);
							//							System.out.println("lower:"+lBound2);
							//							System.out.println("upper:"+uBound2);
							//							System.out.println("refactor:"+reFactor);
							reductionMap.put(tab, (int) (reductionMap.get(tab)*reFactor));//table size after selection
							//							System.out.println("reductionMap:"+reductionMap);
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


						if(plainSelect.getWhere()!=null) {
							List<String> newTableList = new ArrayList<String>();
							List<String> newJoinTableList = new ArrayList<String>();

							for(int index:orderList) {
								newTableList.add(tableList.get(index));
							}
							for(int i =1 ;i<orderList.size();i++) {
								newJoinTableList.add(tableList.get(orderList.get(i)));
							}
							tableList.clear();
							joinedTableList.clear();
							tableList.addAll(newTableList);
							joinedTableList.addAll(joinedTableList);
							//					System.out.println(tableList);
							//					System.out.println(joinedTableList);
							plainSelect.getWhere().accept(joinExtractVisitor);					
							//							System.out.println("ElementList:"+ufVisitor.getUf().getElementList());
							//							System.out.println("residual:"+ufVisitor.getResidualExp());
							//							System.out.println("joinCondition:"+Arrays.toString(App.model.getJoinedTableExp()));
							//							System.out.println("singleTable:"+Arrays.toString(App.model.getsingleTableExp()));
						}
						HashMap<String,Expression> finalSelectionCondition = new HashMap<String,Expression>();
						for(String tab:App.model.getAllTableList()) {
							finalSelectionCondition.put(tab, Tool.genAnds(selectionMap.get(tab)));
						}
						List<Expression> residualJoinCondition = ufVisitor.getResidualJoinExp();
						Expression finalResidualCondition = Tool.genAnds(residualJoinCondition);
						Map<String, List<Expression>> joinMap = classifyJoinCondition(tableList,residualJoinCondition,ufVisitor.getUf());
						System.out.println("JoinMap:"+joinMap);
						//						System.out.println("residualJoinCondition:"+finalResidualCondition );
						//						System.out.println("SelectionCondition:"+finalSelectionCondition);

						//for multiJoin

						List<LogicalOperator> tables = new ArrayList<>();
						LogicalOperator tmp =  new LogicalScanOperator(plainSelect);
						if(finalSelectionCondition.get(App.model.getAllTableList().get(0))!=null) {
							tmp = new LogicalSelectOperator(tmp,plainSelect, finalSelectionCondition.get(App.model.getAllTableList().get(0)));
						}
						tables.add(tmp);


						int tableCount = plainSelect.getJoins().size();
						if(orderList.get(0)==0) {
							//						System.out.println("++firstTableIndex++:"+orderList.get(0));
							leftLogicalOp = new LogicalScanOperator(plainSelect);
						}else {
							int firstTableIndex = orderList.get(0)-1;
							//							System.out.println("firstTableIndex:"+firstTableIndex);
							leftLogicalOp = new LogicalScanOperator(plainSelect,firstTableIndex);//left-most table
						}
						if(finalSelectionCondition.get(tableList.get(0))!=null) {

							Expression singleTableExp = finalSelectionCondition.get(tableList.get(0));
							leftLogicalOp = new LogicalSelectOperator(leftLogicalOp,plainSelect,singleTableExp);

						}
						else {
							leftLogicalOp = new LogicalSelectOperator(leftLogicalOp,plainSelect,null);

						}
						for(int i = 0; i < tableCount; ++i){
							int joinTableIndex = orderList.get(i+1);
							tmp =  new LogicalScanOperator(plainSelect,i);
							if(finalSelectionCondition.get(tableList.get(i+1))!=null) {
								tmp = new LogicalSelectOperator(tmp,plainSelect, finalSelectionCondition.get(tableList.get(i+1)));

								//								System.out.println("whichIndex:"+SelectOptimizer.whichIndexToUse(App.model.getAllTableList().get(i+1), finalSelectionCondition.get(App.model.getAllTableList().get(i+1))));
							}
							tables.add(tmp);
							LogicalOperator LogicalOperatorRight;
							if(joinTableIndex!=0) {
								LogicalOperatorRight = new LogicalScanOperator(plainSelect, joinTableIndex-1);
							}else {
								LogicalOperatorRight = new LogicalScanOperator(plainSelect);
							}
							if(finalSelectionCondition.get(tableList.get(i+1))!=null) {


								LogicalOperatorRight = new LogicalSelectOperator(LogicalOperatorRight,plainSelect,finalSelectionCondition.get(tableList.get(i+1)));
							}
							else {

								LogicalOperatorRight = new LogicalSelectOperator(LogicalOperatorRight,plainSelect,null);
							}
							if(joinMap.get(tableList.get(i+1)).size()!=0) {

								leftLogicalOp = new LogicalJoinOperator(leftLogicalOp, LogicalOperatorRight, plainSelect,Tool.genAnds(joinMap.get(tableList.get(i+1))));
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

						logicalOperator.printTree(System.out, 0);
						PhysicalPlanBuilder ppb = new PhysicalPlanBuilder();
						//						System.out.println("before");
						logicalOperator.accept(ppb);
						//						System.out.println("after");
						operator  = ppb.getOp();
						operator.printTree(System.out, 0);
						operator.printTree(physicalPlanStream, 0);
						physicalPlanStream.close();
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
	public static Map<String, List<Expression>> classifyJoinCondition(List<String> joinOrderList,List<Expression> residualJoinCondition,UnionFind uf) {
		//already in join 
		List<String> alreadyIn = new ArrayList<String>();
		//init joinMap
		Map<String,List<Expression>> joinMap = new HashMap<String,List<Expression>>();
		for(int i =1;i<joinOrderList.size();i++) {
			String table = joinOrderList.get(i);
			joinMap.put(table, new ArrayList<Expression>());
		}
		//store residualJoinCondition
		for(Expression exp:residualJoinCondition) {
			System.out.println("residualexp----inclassify:"+exp);
			BinaryExpression bexp = (BinaryExpression) exp;
			Column cL = (Column) bexp.getLeftExpression();
			Column cR = (Column) bexp.getRightExpression();
			String leftTable =  cL.getTable().toString();
			String rightTable = cR.getTable().toString();
			int leftIndex = joinOrderList.indexOf(leftTable);
			int rightIndex = joinOrderList.indexOf(rightTable);
			if(leftIndex>rightIndex) {
				joinMap.get(leftTable).add(exp);
			}else {
				joinMap.get(rightTable).add(exp);
			}
		}
		//			System.out.println("joinMap--firstPass:"+joinMap);
		//store equalJoinCondition
		Map<String,Element> unionMap = uf.getUnionMap();
		for(String attr: unionMap.keySet()) {
			alreadyIn.add(attr);
			String tableName = attr.split("\\.")[0];
			Element element= uf.findElement(attr);
			//				System.out.println("key:"+attr);
			//				System.out.println("elementAttributeSet:"+element.getColumnAttrs());
			//only one attribute in element
			if(element.getColumnAttrs().size()==1) {
				continue;
			}else {
				for(String attrName:element.getColumnAttrs()) {
					String table = attrName.split("\\.")[0];
					//						System.out.println("table:"+table);
					//						System.out.println("orderList:"+joinOrderList);
					if(!table.equals(tableName)&&!alreadyIn.contains(attrName)) {

						int otherTableIndex = joinOrderList.indexOf(table);
						//							System.out.println("otherTableIndex:"+otherTableIndex);
						int selfTableIndex =  joinOrderList.indexOf(tableName);
						//							System.out.println("selfTableIndex:"+selfTableIndex);
						EqualsTo equalExpression = new EqualsTo();
						Column leftColumn = new Column();
						Table columnLeftTable = new Table();
						Table columnRightTable = new Table();
						columnLeftTable.setName(table);
						columnRightTable.setName(tableName);
						leftColumn.setTable(columnLeftTable);
						leftColumn.setColumnName(attrName.split("\\.")[1]);
						Column rightColumn = new Column();
						rightColumn.setTable(columnRightTable);
						rightColumn.setColumnName(attr.split("\\.")[1]);
						equalExpression.setLeftExpression(leftColumn);
						equalExpression.setRightExpression(rightColumn);
						if(selfTableIndex>otherTableIndex) {
							joinMap.get(tableName).add(equalExpression);
						}else {
							joinMap.get(table).add(equalExpression);
						}
					}
				}
			}
		}



		return joinMap;

	}

}
